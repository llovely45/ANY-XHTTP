package splithttp

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	stdnet "net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/transport/internet/stat"
)

const (
	maxMultiPathCount   = 16
	maxMultiPathFrame   = 1 << 20
	multiPathFrameSize  = 16
	multiPathRetryAfter = 5 * time.Second

	multiPathGroupHeader = "X-Xray-Multipath-Group"
	multiPathLaneHeader  = "X-Xray-Multipath-Lane"
	multiPathCountHeader = "X-Xray-Multipath-Count"
)

var (
	multiPathMagic    = [4]byte{'X', 'M', 'P', '1'}
	multiPathAckMagic = [4]byte{'X', 'M', 'A', '1'}
)

type multiPathContextKey struct{}

type multiPathMeta struct {
	groupID      string
	lane         int
	count        int
	originalDest xnet.Destination
}

func withMultiPathMeta(ctx context.Context, meta multiPathMeta) context.Context {
	return context.WithValue(ctx, multiPathContextKey{}, meta)
}

func multiPathMetaFromContext(ctx context.Context) (multiPathMeta, bool) {
	meta, ok := ctx.Value(multiPathContextKey{}).(multiPathMeta)
	return meta, ok
}

func applyMultiPathHeaders(header mapHeader, ctx context.Context) {
	meta, ok := multiPathMetaFromContext(ctx)
	if !ok {
		return
	}
	header.Set(multiPathGroupHeader, meta.groupID)
	header.Set(multiPathLaneHeader, strconv.Itoa(meta.lane))
	header.Set(multiPathCountHeader, strconv.Itoa(meta.count))
}

// mapHeader is the subset of net/http.Header used here, which avoids making
// the transport framing type depend on net/http.
type mapHeader interface {
	Set(string, string)
}

func parseMultiPathHeaders(get func(string) string) (*multiPathMeta, error) {
	group := get(multiPathGroupHeader)
	laneText := get(multiPathLaneHeader)
	countText := get(multiPathCountHeader)
	if group == "" && laneText == "" && countText == "" {
		return nil, nil
	}
	if !validMultiPathGroupID(group) {
		return nil, errors.New("invalid multipath group identifier")
	}
	lane, laneErr := strconv.Atoi(laneText)
	count, countErr := strconv.Atoi(countText)
	if laneErr != nil || countErr != nil || count < 2 || count > maxMultiPathCount || lane < 0 || lane >= count {
		return nil, errors.New("invalid multipath lane metadata")
	}
	return &multiPathMeta{groupID: group, lane: lane, count: count}, nil
}

func validMultiPathGroupID(id string) bool {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return false
	}
	for i, b := range id {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if !((b >= '0' && b <= '9') || (b >= 'a' && b <= 'f')) {
			return false
		}
	}
	return true
}

func newMultiPathGroupID() (string, error) {
	var id [16]byte
	if _, err := cryptorand.Read(id[:]); err != nil {
		return "", err
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		id[0:4], id[4:6], id[6:8], id[8:10], id[10:16]), nil
}

type multiPathOutbound struct {
	frame   []byte
	pending *multiPathPendingFrame
}

type multiPathLane struct {
	conn   stat.Connection
	out    chan multiPathOutbound
	queued atomic.Int64
	dead   atomic.Bool
}

type multiPathPendingFrame struct {
	offset      uint64
	frame       []byte
	lane        int
	received    bool
	lastAttempt time.Time
}

type multiPathConn struct {
	lanes     []*multiPathLane
	chunkSize int
	maxBuffer int
	nextLane  int
	done      chan struct{}

	readMu       sync.Mutex
	mu           sync.Mutex
	readCond     *sync.Cond
	readBuffer   []byte
	readPosition int
	pending      map[uint64][]byte
	readOffset   uint64
	consumed     uint64
	buffered     int
	err          error

	writeMu      sync.Mutex
	writeOffset  uint64
	receivedAck  uint64
	consumedAck  uint64
	sendPending  map[uint64]*multiPathPendingFrame
	sendBuffered int
}

func newMultiPathConn(conns []stat.Connection, config *MultiPathConfig) (stat.Connection, error) {
	if len(conns) < 2 || len(conns) > maxMultiPathCount {
		return nil, fmt.Errorf("multipath requires between 2 and %d lanes", maxMultiPathCount)
	}
	chunkSize, maxBuffer := normalizedMultiPathBuffer(config)
	c := &multiPathConn{
		lanes:       make([]*multiPathLane, len(conns)),
		chunkSize:   chunkSize,
		maxBuffer:   maxBuffer,
		done:        make(chan struct{}),
		pending:     make(map[uint64][]byte),
		sendPending: make(map[uint64]*multiPathPendingFrame),
	}
	c.readCond = sync.NewCond(&c.mu)
	for i, conn := range conns {
		c.lanes[i] = &multiPathLane{conn: conn, out: make(chan multiPathOutbound, 64)}
	}
	for _, lane := range c.lanes {
		go c.writeLane(lane)
		go c.readLane(lane)
	}
	go c.retransmitUnacknowledged()
	return c, nil
}

func normalizedMultiPathBuffer(config *MultiPathConfig) (int, int) {
	chunkSize := 16 * 1024
	maxBuffer := 4 * 1024 * 1024
	if config != nil {
		if config.ChunkSize >= 1024 && config.ChunkSize <= maxMultiPathFrame {
			chunkSize = int(config.ChunkSize)
		}
		if config.MaxBufferSize >= uint32(2*chunkSize) && config.MaxBufferSize <= 64*1024*1024 {
			maxBuffer = int(config.MaxBufferSize)
		}
	}
	return chunkSize, maxBuffer
}

func (c *multiPathConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.readMu.Lock()
	defer c.readMu.Unlock()

	c.mu.Lock()
	for c.readPosition >= len(c.readBuffer) && c.err == nil {
		c.readCond.Wait()
	}
	if c.readPosition < len(c.readBuffer) {
		n := copy(p, c.readBuffer[c.readPosition:])
		c.readPosition += n
		c.buffered -= n
		c.consumed += uint64(n)
		receivedOffset := c.readOffset
		consumedOffset := c.consumed
		if c.readPosition == len(c.readBuffer) {
			c.readBuffer = c.readBuffer[:0]
			c.readPosition = 0
		} else if c.readPosition >= c.chunkSize*2 {
			c.readBuffer = append(c.readBuffer[:0], c.readBuffer[c.readPosition:]...)
			c.readPosition = 0
		}
		c.readCond.Broadcast()
		c.mu.Unlock()
		_ = c.enqueueControl(encodeMultiPathAck(receivedOffset, consumedOffset))
		return n, nil
	}
	err := c.err
	c.mu.Unlock()
	return 0, err
}

func (c *multiPathConn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	written := 0
	for written < len(p) {
		n := min(c.chunkSize, len(p)-written)
		c.mu.Lock()
		for c.sendBuffered+n > c.maxBuffer && c.err == nil {
			c.readCond.Wait()
		}
		if c.err != nil {
			err := c.err
			c.mu.Unlock()
			return written, err
		}

		offset := c.writeOffset
		frame := encodeMultiPathFrame(offset, p[written:written+n])
		pending := &multiPathPendingFrame{offset: offset, frame: frame, lane: -1, lastAttempt: time.Now()}
		c.sendPending[offset] = pending
		c.writeOffset += uint64(n)
		c.sendBuffered += n
		c.mu.Unlock()

		if err := c.enqueueData(pending); err != nil {
			return written, err
		}
		written += n
	}
	return written, nil
}

func encodeMultiPathFrame(offset uint64, payload []byte) []byte {
	frame := make([]byte, multiPathFrameSize+len(payload))
	copy(frame[:4], multiPathMagic[:])
	binary.BigEndian.PutUint64(frame[4:12], offset)
	binary.BigEndian.PutUint32(frame[12:16], uint32(len(payload)))
	copy(frame[multiPathFrameSize:], payload)
	return frame
}

func encodeMultiPathAck(receivedOffset, consumedOffset uint64) []byte {
	frame := make([]byte, multiPathFrameSize+8)
	copy(frame[:4], multiPathAckMagic[:])
	binary.BigEndian.PutUint64(frame[4:12], receivedOffset)
	binary.BigEndian.PutUint32(frame[12:16], 8)
	binary.BigEndian.PutUint64(frame[multiPathFrameSize:], consumedOffset)
	return frame
}

func (c *multiPathConn) enqueueData(pending *multiPathPendingFrame) error {
	for {
		c.mu.Lock()
		if c.err != nil {
			err := c.err
			c.mu.Unlock()
			return err
		}
		if c.sendPending[pending.offset] != pending {
			c.mu.Unlock()
			return nil
		}
		if pending.lane >= 0 {
			c.mu.Unlock()
			return nil
		}
		laneIndex := c.leastQueuedLaneLocked(-1)
		if laneIndex >= 0 {
			lane := c.lanes[laneIndex]
			lane.queued.Add(int64(len(pending.frame)))
			pending.lane = laneIndex
			select {
			case lane.out <- multiPathOutbound{frame: pending.frame, pending: pending}:
				pending.lastAttempt = time.Now()
				c.mu.Unlock()
				return nil
			default:
				lane.queued.Add(-int64(len(pending.frame)))
				pending.lane = -1
			}
		}
		c.mu.Unlock()

		select {
		case <-c.done:
			return c.currentError()
		case <-time.After(time.Millisecond):
		}
	}
}

func (c *multiPathConn) enqueueControl(frame []byte) error {
	for {
		c.mu.Lock()
		if c.err != nil {
			err := c.err
			c.mu.Unlock()
			return err
		}
		laneIndex := c.leastQueuedLaneLocked(-1)
		if laneIndex >= 0 {
			lane := c.lanes[laneIndex]
			lane.queued.Add(int64(len(frame)))
			select {
			case lane.out <- multiPathOutbound{frame: frame}:
				c.mu.Unlock()
				return nil
			default:
				lane.queued.Add(-int64(len(frame)))
			}
		}
		c.mu.Unlock()

		select {
		case <-c.done:
			return c.currentError()
		case <-time.After(time.Millisecond):
		}
	}
}

func (c *multiPathConn) leastQueuedLaneLocked(exclude int) int {
	selected := -1
	var selectedQueue int64
	for step := range len(c.lanes) {
		i := (c.nextLane + step) % len(c.lanes)
		lane := c.lanes[i]
		if i == exclude || lane.dead.Load() {
			continue
		}
		queued := lane.queued.Load()
		if selected < 0 || queued < selectedQueue {
			selected = i
			selectedQueue = queued
		}
	}
	if selected >= 0 {
		c.nextLane = (selected + 1) % len(c.lanes)
	}
	return selected
}

func (c *multiPathConn) writeLane(lane *multiPathLane) {
	for {
		select {
		case <-c.done:
			return
		case outbound := <-lane.out:
			if lane.dead.Load() {
				continue
			}
			err := writeFull(lane.conn, outbound.frame)
			lane.queued.Add(-int64(len(outbound.frame)))
			if err != nil {
				c.laneFailed(lane, err)
				return
			}
			if outbound.pending != nil {
				c.mu.Lock()
				if c.sendPending[outbound.pending.offset] == outbound.pending {
					outbound.pending.lastAttempt = time.Now()
				}
				c.mu.Unlock()
			}
		}
	}
}

func writeFull(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		p = p[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (c *multiPathConn) readLane(lane *multiPathLane) {
	var header [multiPathFrameSize]byte
	for {
		if _, err := io.ReadFull(lane.conn, header[:]); err != nil {
			c.laneFailed(lane, err)
			return
		}
		offset := binary.BigEndian.Uint64(header[4:12])
		length := binary.BigEndian.Uint32(header[12:16])
		switch string(header[:4]) {
		case string(multiPathAckMagic[:]):
			if length != 8 {
				c.fail(fmt.Errorf("invalid multipath acknowledgement length %d", length))
				return
			}
			var payload [8]byte
			if _, err := io.ReadFull(lane.conn, payload[:]); err != nil {
				c.laneFailed(lane, err)
				return
			}
			consumedOffset := binary.BigEndian.Uint64(payload[:])
			if err := c.acceptAck(offset, consumedOffset); err != nil {
				c.fail(err)
				return
			}
		case string(multiPathMagic[:]):
			if length == 0 || length > maxMultiPathFrame {
				c.fail(fmt.Errorf("invalid multipath frame length %d", length))
				return
			}
			payload := make([]byte, int(length))
			if _, err := io.ReadFull(lane.conn, payload); err != nil {
				c.laneFailed(lane, err)
				return
			}
			receivedOffset, consumedOffset, err := c.acceptFrame(offset, payload)
			if err != nil {
				c.fail(err)
				return
			}
			_ = c.enqueueControl(encodeMultiPathAck(receivedOffset, consumedOffset))
		default:
			c.fail(fmt.Errorf("invalid multipath frame magic"))
			return
		}
	}
}

func (c *multiPathConn) acceptFrame(offset uint64, payload []byte) (uint64, uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return c.readOffset, c.consumed, c.err
	}
	end := offset + uint64(len(payload))
	if end < offset {
		return c.readOffset, c.consumed, errors.New("multipath frame offset overflow")
	}
	if offset < c.readOffset {
		if end <= c.readOffset {
			return c.readOffset, c.consumed, nil
		}
		return c.readOffset, c.consumed, fmt.Errorf("multipath frame overlaps delivered data at offset %d", offset)
	}
	if existing, ok := c.pending[offset]; ok {
		if string(existing) != string(payload) {
			return c.readOffset, c.consumed, fmt.Errorf("conflicting multipath frame at offset %d", offset)
		}
		return c.readOffset, c.consumed, nil
	}
	for pendingOffset, existing := range c.pending {
		pendingEnd := pendingOffset + uint64(len(existing))
		if offset < pendingEnd && pendingOffset < end {
			return c.readOffset, c.consumed, fmt.Errorf("overlapping multipath frames at offsets %d and %d", pendingOffset, offset)
		}
	}
	if c.buffered+len(payload) > c.maxBuffer {
		return c.readOffset, c.consumed, fmt.Errorf("multipath receive window exceeded: %d > %d", c.buffered+len(payload), c.maxBuffer)
	}
	if offset == c.readOffset {
		c.readBuffer = append(c.readBuffer, payload...)
		c.buffered += len(payload)
		c.readOffset = end
		for {
			next, ok := c.pending[c.readOffset]
			if !ok {
				break
			}
			delete(c.pending, c.readOffset)
			c.readBuffer = append(c.readBuffer, next...)
			c.readOffset += uint64(len(next))
		}
		c.readCond.Broadcast()
		return c.readOffset, c.consumed, nil
	}
	c.pending[offset] = payload
	c.buffered += len(payload)
	return c.readOffset, c.consumed, nil
}

func (c *multiPathConn) acceptAck(receivedOffset, consumedOffset uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if receivedOffset > c.writeOffset || consumedOffset > receivedOffset {
		return fmt.Errorf("invalid multipath acknowledgement: received %d, consumed %d, sent %d", receivedOffset, consumedOffset, c.writeOffset)
	}
	if receivedOffset > c.receivedAck {
		c.receivedAck = receivedOffset
	}
	if consumedOffset > c.consumedAck {
		c.consumedAck = consumedOffset
	}
	for frameOffset, pending := range c.sendPending {
		frameLength := uint64(len(pending.frame) - multiPathFrameSize)
		frameEnd := frameOffset + frameLength
		if frameEnd <= c.consumedAck {
			delete(c.sendPending, frameOffset)
		} else if frameEnd <= c.receivedAck {
			pending.received = true
		}
	}
	c.sendBuffered = int(c.writeOffset - c.consumedAck)
	c.readCond.Broadcast()
	return nil
}

func (c *multiPathConn) retransmitUnacknowledged() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case now := <-ticker.C:
			c.mu.Lock()
			if c.err != nil {
				c.mu.Unlock()
				return
			}
			windowBlocked := c.sendBuffered >= c.maxBuffer
			var retry []*multiPathPendingFrame
			for _, pending := range c.sendPending {
				if now.Sub(pending.lastAttempt) < multiPathRetryAfter || (pending.received && !windowBlocked) {
					continue
				}
				pending.lane = -1
				pending.lastAttempt = now
				retry = append(retry, pending)
				if pending.received {
					// One duplicate is enough to request a fresh consumption ACK.
					break
				}
			}
			c.mu.Unlock()
			for _, pending := range retry {
				if err := c.enqueueData(pending); err != nil {
					return
				}
			}
		}
	}
}

func (c *multiPathConn) laneFailed(lane *multiPathLane, err error) {
	c.mu.Lock()
	if !lane.dead.CompareAndSwap(false, true) {
		c.mu.Unlock()
		return
	}
	active := 0
	for _, current := range c.lanes {
		if !current.dead.Load() {
			active++
		}
	}
	if active == 0 {
		c.setTerminalErrorLocked(fmt.Errorf("all multipath lanes failed: %w", err))
		conns := c.markAllLanesDeadLocked()
		c.mu.Unlock()
		_ = lane.conn.Close()
		for _, conn := range conns {
			_ = conn.Close()
		}
		return
	}

	toResend := make([]*multiPathPendingFrame, 0, len(c.sendPending))
	for _, pending := range c.sendPending {
		pending.lane = -1
		toResend = append(toResend, pending)
	}
	c.readCond.Broadcast()
	c.mu.Unlock()
	_ = lane.conn.Close()
	for _, pending := range toResend {
		if err := c.enqueueData(pending); err != nil {
			return
		}
	}
}

func (c *multiPathConn) fail(err error) {
	c.mu.Lock()
	if c.err != nil {
		c.mu.Unlock()
		return
	}
	c.setTerminalErrorLocked(err)
	conns := c.markAllLanesDeadLocked()
	c.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (c *multiPathConn) setTerminalErrorLocked(err error) {
	if c.err != nil {
		return
	}
	c.err = err
	close(c.done)
	c.readCond.Broadcast()
}

func (c *multiPathConn) markAllLanesDeadLocked() []stat.Connection {
	var conns []stat.Connection
	for _, lane := range c.lanes {
		if lane.dead.CompareAndSwap(false, true) {
			conns = append(conns, lane.conn)
		}
	}
	return conns
}

func (c *multiPathConn) currentError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *multiPathConn) Close() error {
	c.fail(stdnet.ErrClosed)
	return nil
}

func (c *multiPathConn) LocalAddr() stdnet.Addr {
	if len(c.lanes) == 0 {
		return nil
	}
	return c.lanes[0].conn.LocalAddr()
}

func (c *multiPathConn) RemoteAddr() stdnet.Addr {
	if len(c.lanes) == 0 {
		return nil
	}
	return c.lanes[0].conn.RemoteAddr()
}

func (c *multiPathConn) SetDeadline(t time.Time) error {
	return c.setDeadline(func(conn stat.Connection) error { return conn.SetDeadline(t) })
}

func (c *multiPathConn) SetReadDeadline(t time.Time) error {
	return c.setDeadline(func(conn stat.Connection) error { return conn.SetReadDeadline(t) })
}

func (c *multiPathConn) SetWriteDeadline(t time.Time) error {
	return c.setDeadline(func(conn stat.Connection) error { return conn.SetWriteDeadline(t) })
}

func (c *multiPathConn) setDeadline(set func(stat.Connection) error) error {
	var firstErr error
	for _, lane := range c.lanes {
		if err := set(lane.conn); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func multipathEnabled(config *MultiPathConfig) bool {
	return config != nil && config.Enabled
}

func normalizedMultiPathMaxPaths(config *MultiPathConfig) int {
	if config == nil || config.MaxPaths == 0 {
		return 8
	}
	return min(int(config.MaxPaths), maxMultiPathCount)
}
