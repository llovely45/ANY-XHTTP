package splithttp

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/transport/internet/stat"
)

const multiPathGroupTombstoneTTL = 5 * time.Minute

type multiPathGroup struct {
	mu       sync.Mutex
	id       string
	expected int
	lanes    map[int]*multiPathGroupLane
	timer    *time.Timer
	done     bool
}

type multiPathGroupLane struct {
	conn stat.Connection
}

func (h *requestHandler) addMultiPathLane(meta multiPathMeta, conn stat.Connection) *multiPathGroupLane {
	timeoutSeconds := uint32(15)
	if h.config.Multipath != nil && h.config.Multipath.HandshakeTimeoutSeconds > 0 {
		timeoutSeconds = h.config.Multipath.HandshakeTimeoutSeconds
	}
	group := &multiPathGroup{
		id:       meta.groupID,
		expected: meta.count,
		lanes:    make(map[int]*multiPathGroupLane, meta.count),
	}
	group.timer = time.AfterFunc(time.Duration(timeoutSeconds)*time.Second, func() {
		h.finishMultiPathGroup(group, true)
	})

	actual, loaded := h.multiPathGroups.LoadOrStore(meta.groupID, group)
	if loaded {
		group.timer.Stop()
		group = actual.(*multiPathGroup)
	}

	group.mu.Lock()
	if group.done {
		group.mu.Unlock()
		_ = conn.Close()
		return nil
	}
	if group.expected != meta.count {
		group.mu.Unlock()
		_ = conn.Close()
		errors.LogInfo(context.Background(), "multipath group count mismatch for ", meta.groupID)
		return nil
	}
	if _, exists := group.lanes[meta.lane]; exists {
		group.mu.Unlock()
		_ = conn.Close()
		errors.LogInfo(context.Background(), "duplicate multipath lane ", meta.lane, " for ", meta.groupID)
		return nil
	}
	lane := &multiPathGroupLane{conn: conn}
	group.lanes[meta.lane] = lane
	complete := len(group.lanes) == group.expected
	group.mu.Unlock()
	if complete {
		h.finishMultiPathGroup(group, false)
	}
	return lane
}

func (h *requestHandler) removeMultiPathLane(group *multiPathGroup, laneIndex int, lane *multiPathGroupLane) {
	if group == nil || lane == nil {
		return
	}
	group.mu.Lock()
	if !group.done && group.lanes[laneIndex] == lane {
		delete(group.lanes, laneIndex)
	}
	group.mu.Unlock()
}

func (h *requestHandler) finishMultiPathGroup(group *multiPathGroup, allowPartial bool) {
	group.mu.Lock()
	if group.done || (!allowPartial && len(group.lanes) != group.expected) {
		group.mu.Unlock()
		return
	}
	group.done = true
	if group.timer != nil {
		group.timer.Stop()
	}
	laneIndexes := make([]int, 0, len(group.lanes))
	for laneIndex := range group.lanes {
		laneIndexes = append(laneIndexes, laneIndex)
	}
	sort.Ints(laneIndexes)
	conns := make([]stat.Connection, 0, len(laneIndexes))
	for _, laneIndex := range laneIndexes {
		conns = append(conns, group.lanes[laneIndex].conn)
	}
	partial := len(group.lanes) < group.expected
	group.lanes = nil
	group.mu.Unlock()
	if partial {
		time.AfterFunc(multiPathGroupTombstoneTTL, func() {
			h.multiPathGroups.CompareAndDelete(group.id, group)
		})
	} else {
		h.multiPathGroups.CompareAndDelete(group.id, group)
	}

	if len(conns) < 2 {
		for _, conn := range conns {
			_ = conn.Close()
		}
		return
	}
	conn, err := newMultiPathConn(conns, h.config.Multipath)
	if err != nil {
		for _, laneConn := range conns {
			_ = laneConn.Close()
		}
		errors.LogInfoInner(context.Background(), err, "failed to create server-side multipath connection")
		return
	}
	h.ln.addConn(conn)
}
