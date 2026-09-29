# XHTTP multipath

XHTTP multipath stripes one ordered byte stream over several independent XHTTP
connections. Each connection is pinned to one IP address for the Xray server;
the server joins the lanes before handing the stream to the proxy inbound.
Both peers must run a build that supports this extension and enable it in their
XHTTP settings.

On the client, omit `addresses` to resolve the Xray server's `address` through
Xray DNS and use up to `maxPaths` returned IP addresses. This resolves the
server address only. The configured XHTTP `host`, TLS `serverName`, and REALITY
`serverName` keep their normal precedence and values; the selected IP is only
used for the underlying connection. `addresses` can be set to an explicit list
of IPs instead.

Example client transport settings:

```json
{
  "network": "xhttp",
  "security": "tls",
  "tlsSettings": {
    "serverName": "tls-front.example.net"
  },
  "xhttpSettings": {
    "host": "http-front.example.net",
    "path": "/xhttp",
    "multipath": {
      "enabled": true,
      "maxPaths": 4,
      "chunkSize": 16384,
      "maxBufferSize": 4194304,
      "handshakeTimeoutSeconds": 15
    }
  }
}
```

The server's XHTTP inbound needs `"multipath": {"enabled": true}` as well.
Use the same `chunkSize` and `maxBufferSize` on both peers. The server's
`maxPaths` must be at least the number of lanes the client may open. The CDN or
reverse proxy must forward the `X-Xray-Multipath-*` request headers and allow
the long-lived XHTTP streams used by the configured mode. All lanes in one
group must reach the same Xray server process; if a load balancer can route
lanes to different Xray instances, configure affinity for the group or use a
single backend because the server keeps group state in memory.

Data is framed with stream offsets, reordered at the receiver, acknowledged,
and retransmitted over another active lane when a frame is not acknowledged
within five seconds. A detected lane failure also moves unconsumed frames to
the remaining lanes. Ordinary packet loss is normally recovered by the
underlying TCP or QUIC connection; reordering can still make the stream wait
for a slower lane. If fewer than two IPs resolve, or not all lanes connect
during setup, the client falls back to one XHTTP connection. If every active
lane fails, the Xray connection closes.

Multipath adds framing, acknowledgements, and duplicate traffic during
recovery. It does not guarantee that CDN IPs reach different points of
presence or that their bandwidth will add up; test the actual route and
throughput before using it in production. `downloadSettings` and the browser
dialer are not supported with multipath yet.

## Client builds

The `Build XHTTP multipath clients` GitHub Actions workflow builds v2rayNG for
Android and v2rayN for Windows x64/ARM64 and macOS Intel/Apple Silicon. It
compiles `libv2ray.aar` against the checked-out Xray source and replaces the
Xray core bundled in each v2rayN package. The generated clients accept the
`multipath` object through their existing XHTTP Extra JSON field.

In either client's XHTTP Extra editor, enter an object such as
`{"multipath":{"enabled":true,"maxPaths":4}}`. Leave `addresses` out to
resolve the node's server address automatically.

The Android APK uses v2rayNG's F-Droid application ID so it can be installed
beside the Play Store build. It is signed with the runner's debug key, so a
later workflow run may require uninstalling this custom build before installing
the new one. Windows and macOS packages are unsigned. Updating the Xray core
from inside v2rayN replaces the custom core, so rebuild the package to restore
multipath support. Workflow artifacts are available for 30 days from the
Actions run.
