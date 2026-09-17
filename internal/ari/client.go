// Package ari wraps the Asterisk REST Interface: one WebSocket connection for the
// whole process, with reconnect-with-backoff at startup and reconnect observability
// for the life of the process.
package ari

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/client/native"

	"github.com/nitesh/vaani/internal/config"
	"github.com/nitesh/vaani/internal/metrics"
)

const (
	initialBackoff = 500 * time.Millisecond
	maxBackoff     = 30 * time.Second

	// waitingLogInterval controls how often we remind the operator we're still
	// trying, since native.Connect can block silently for a long time (see below).
	waitingLogInterval = 5 * time.Second
)

// connectResult carries the outcome of one native.Connect attempt back from the
// goroutine it runs in.
type connectResult struct {
	cl  ari.Client
	err error
}

// Connect establishes the ARI client, retrying the initial connection with
// exponential backoff (capped at 30s) until ctx is cancelled or it succeeds. Once
// connected, the native client's own read loop keeps the WebSocket alive and
// reconnects transparently on the same Bus subscriptions; watchReconnects observes
// those drops for the vaani_ari_reconnects_total metric.
//
// native.Connect itself takes no context and, when Asterisk is unreachable, blocks
// forever inside its own internal retry loop rather than returning an error -- so
// every attempt here runs in its own goroutine, raced against ctx.Done(), purely so
// that cancelling ctx (SIGTERM, Ctrl+C) can always return promptly. An attempt that
// never finishes is abandoned, not killed -- its goroutine dies with the process.
func Connect(ctx context.Context, cfg config.Config) (ari.Client, error) {
	wsURL, err := websocketURL(cfg.AriURL)
	if err != nil {
		return nil, fmt.Errorf("ari: %w", err)
	}

	opts := &native.Options{
		Application:  cfg.AriApp,
		URL:          cfg.AriURL,
		WebsocketURL: wsURL,
		Username:     cfg.AriUser,
		Password:     cfg.AriPass,
	}

	backoff := initialBackoff

	for {
		resCh := make(chan connectResult, 1)

		go func() {
			cl, err := native.Connect(opts)
			resCh <- connectResult{cl, err}
		}()

		result, cancelled := waitForConnect(ctx, resCh, cfg.AriURL)
		if cancelled {
			return nil, ctx.Err()
		}

		if result.err == nil {
			go watchReconnects(ctx, result.cl)
			return result.cl, nil
		}

		slog.Warn("ari connect failed, retrying", "error", result.err, "backoff", backoff)

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// waitForConnect blocks until resCh delivers a result or ctx is cancelled (in which
// case cancelled is true and result is zero), periodically logging so a long,
// silent native.Connect attempt (e.g. Asterisk simply isn't reachable yet) doesn't
// look like a hang.
func waitForConnect(ctx context.Context, resCh <-chan connectResult, ariURL string) (result connectResult, cancelled bool) {
	ticker := time.NewTicker(waitingLogInterval)
	defer ticker.Stop()

	for {
		select {
		case r := <-resCh:
			return r, false
		case <-ticker.C:
			slog.Info("still waiting for Asterisk ARI", "url", ariURL)
		case <-ctx.Done():
			return connectResult{}, true
		}
	}
}

// watchReconnects polls Connected() and increments the reconnect counter on every
// false->true transition, logging both directions.
func watchReconnects(ctx context.Context, cl ari.Client) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	up := cl.Connected()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := cl.Connected()
			if now && !up {
				metrics.ARIReconnects.Inc()
				slog.Info("ari websocket reconnected")
			} else if !now && up {
				slog.Warn("ari websocket disconnected")
			}

			up = now
		}
	}
}

// websocketURL derives the ARI events WebSocket URL from the REST base URL, e.g.
// http://host:8088/ari -> ws://host:8088/ari/events.
func websocketURL(ariURL string) (string, error) {
	u, err := url.Parse(ariURL)
	if err != nil {
		return "", fmt.Errorf("invalid ARI_URL %q: %w", ariURL, err)
	}

	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	default:
		u.Scheme = "ws"
	}

	u.Path = strings.TrimSuffix(u.Path, "/") + "/events"

	return u.String(), nil
}
