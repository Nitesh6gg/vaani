package ari

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/config"
)

// TestConnect_CancelDuringHangingAttempt is the regression test for the bug where
// Ctrl+C/SIGTERM did nothing while Asterisk was unreachable: native.Connect blocks
// forever (no context, no timeout) against a server that accepts the TCP connection
// but never responds, and before the fix, nothing raced that against ctx.Done().
func TestConnect_CancelDuringHangingAttempt(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	// Accept connections and then go silent -- never write a response, so any HTTP
	// or WebSocket handshake against this address hangs indefinitely, exactly like
	// a network-level black hole (unreachable host, dropped firewall rule, etc).
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			go func(c net.Conn) {
				buf := make([]byte, 4096)
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	cfg := config.Config{
		AriURL:  "http://" + ln.Addr().String() + "/ari",
		AriUser: "vaani",
		AriPass: "vaani",
		AriApp:  "vaani",
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err = Connect(ctx, cfg)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, elapsed, 2*time.Second,
		"Connect must return promptly on cancellation even while an attempt is hung, took %v", elapsed)
}
