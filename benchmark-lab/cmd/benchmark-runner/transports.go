package main

// Blank imports trigger each protocol package's init() which calls scenarios.Register().
// Without these imports the factories map stays empty and every run fails with
// "unknown protocol".
import (
	_ "github.com/himanshuplace/protocol_for_broadcast/internal/http1"
	_ "github.com/himanshuplace/protocol_for_broadcast/internal/http2"
	_ "github.com/himanshuplace/protocol_for_broadcast/internal/http3"
	_ "github.com/himanshuplace/protocol_for_broadcast/internal/sse"
	_ "github.com/himanshuplace/protocol_for_broadcast/internal/tcp"
	_ "github.com/himanshuplace/protocol_for_broadcast/internal/udp"
	_ "github.com/himanshuplace/protocol_for_broadcast/internal/webrtc"
	_ "github.com/himanshuplace/protocol_for_broadcast/internal/webtransport"
	_ "github.com/himanshuplace/protocol_for_broadcast/internal/websocket/coder"
	_ "github.com/himanshuplace/protocol_for_broadcast/internal/websocket/gobwas"
	_ "github.com/himanshuplace/protocol_for_broadcast/internal/websocket/gorilla"
)
