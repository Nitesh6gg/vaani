package ari

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/nitesh/vaani/internal/config"
)

// externalMediaHTTPClient is dedicated to the one raw REST call in this file, with
// a bounded timeout so a hung ARI server can't block call setup forever. This
// isn't the "shared http.Client for hot-path provider calls" invariant from
// CLAUDE.md -- that's about high-frequency AI provider traffic; this fires once
// per call, on setup.
var externalMediaHTTPClient = &http.Client{Timeout: 10 * time.Second}

// createAudioSocketExternalMedia creates an externalMedia channel with
// encapsulation=audiosocket, working around a real gap in the vendored ARI
// library: ari.ExternalMediaOptions has no "data" field at all, but Asterisk's
// chan_audiosocket rejects the request without one -- confirmed live via `ari set
// debug on`: "400 Bad Request: data can not be empty". That field is the UUID
// Asterisk echoes back as the connection's first AudioSocket frame (kind 0x01).
// Since the library gives no way to set it, this makes the raw REST call directly
// instead of going through Channel().ExternalMedia() (which the RTP path still
// uses, unchanged).
func createAudioSocketExternalMedia(cfg config.Config, channelID, app, externalHost, uuid string) error {
	body := map[string]any{
		"channelId":       channelID,
		"app":             app,
		"external_host":   externalHost,
		"encapsulation":   "audiosocket",
		"transport":       "tcp",
		"connection_type": "client",
		"format":          "slin16",
		"direction":       "both",
		"data":            uuid,
	}

	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal externalMedia request: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, cfg.AriURL+"/channels/externalMedia", bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("build externalMedia request: %w", err)
	}

	req.SetBasicAuth(cfg.AriUser, cfg.AriPass)
	req.Header.Set("Content-Type", "application/json")

	resp, err := externalMediaHTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("externalMedia request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("externalMedia: non-2xx response: %s", resp.Status)
	}

	return nil
}

// newUUIDv4 generates a random RFC 4122 version-4 UUID string. No external
// dependency for something this small: 16 random bytes, the version/variant bits
// set per spec, formatted as the standard 8-4-4-4-12 hex string.
func newUUIDv4() (string, error) {
	var b [16]byte

	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate uuid: %w", err)
	}

	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10

	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
