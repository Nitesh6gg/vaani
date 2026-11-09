package ari

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/nitesh/vaani/internal/config"
	"github.com/nitesh/vaani/internal/metrics"
)

// orphanMinAge is how old an untracked channel must be before the sweep hangs
// it up. Setup is given 15 seconds (setupTimeout), so anything this old that
// Vaani doesn't know about is lost, not in progress.
const orphanMinAge = 90 * time.Second

// ariChannel is the part of Asterisk's channel object the sweep reads.
type ariChannel struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	CreationTime string `json:"creationtime"`
	Dialplan     struct {
		AppName string `json:"app_name"`
		AppData string `json:"app_data"`
	} `json:"dialplan"`
}

// listChannels is GET /channels: every channel Asterisk has.
func listChannels(ctx context.Context, cfg config.Config) ([]ariChannel, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, cfg.AriURL+"/channels", nil)
	if err != nil {
		return nil, fmt.Errorf("build channels request: %w", err)
	}

	req.SetBasicAuth(cfg.AriUser, cfg.AriPass)

	resp, err := externalMediaHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, responseError("list channels", resp)
	}

	var chans []ariChannel
	if err := json.NewDecoder(resp.Body).Decode(&chans); err != nil {
		return nil, fmt.Errorf("list channels: %w", err)
	}

	return chans, nil
}

// created parses a channel's creationtime ("2026-11-09T11:29:50.123+0000").
func (c ariChannel) created() (time.Time, bool) {
	for _, layout := range []string{"2006-01-02T15:04:05.000-0700", "2006-01-02T15:04:05-0700", time.RFC3339Nano} {
		if t, err := time.Parse(layout, c.CreationTime); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}

// sweepOrphans every ORPHAN_SWEEP_SECONDS hangs up the channels of Vaani's own
// Stasis app that Vaani lost track of: a call whose events were lost, or whose
// setup died half way, leaves its channels Up in Stasis on Asterisk forever
// (billable, and counted against the trunk's channel limit), with nothing left
// to ever hang them up. Assumes this Vaani is the only one on its ARI app name.
func (m *Manager) sweepOrphans(ctx context.Context) {
	if m.cfg.OrphanSweepInterval <= 0 {
		return
	}

	t := time.NewTicker(m.cfg.OrphanSweepInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		if !m.cl.Connected() {
			continue
		}

		if _, err := m.sweepOnce(ctx, time.Now()); err != nil {
			slog.Warn("orphan channel sweep failed", "event", "channel.sweep_failed", "error", err)
		}
	}
}

// sweepOnce hangs up the orphans among Asterisk's channels and returns their IDs.
func (m *Manager) sweepOnce(ctx context.Context, now time.Time) ([]string, error) {
	chans, err := listChannels(ctx, m.cfg)
	if err != nil {
		return nil, err
	}

	var orphans []string

	for _, ch := range chans {
		app := ch.Dialplan.AppData
		if ch.Dialplan.AppName != "Stasis" || (app != m.cfg.AriApp && !strings.HasPrefix(app, m.cfg.AriApp+",")) {
			continue // not ours
		}

		if created, ok := ch.created(); !ok || now.Sub(created) < orphanMinAge {
			continue // too young, or not sure
		}

		if m.tracks(ch.ID) {
			continue
		}

		slog.Warn("hanging up a channel Vaani lost track of", "call_id", ch.ID, "event", "channel.orphan", "name", ch.Name,
			"age_s", int(now.Sub(mustTime(ch.created())).Seconds()))
		metrics.OrphanChannelsTotal.Inc()
		hangupChannel(m.cl, ch.ID, ch.ID)

		orphans = append(orphans, ch.ID)
	}

	return orphans, nil
}

func mustTime(t time.Time, _ bool) time.Time { return t }

// tracks says whether Vaani knows channel id: a call being set up or bridged
// (either leg, or a transfer destination), a transfer ringing, or a call it dialed.
func (m *Manager) tracks(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.pending[id] != nil || m.active[id] != nil || m.transfers[id] != nil || m.outbound[id] != nil {
		return true
	}

	return false
}
