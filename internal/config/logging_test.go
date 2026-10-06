package config

import (
	"bytes"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLogTimestampIsUTCWithMilliseconds(t *testing.T) {
	var buf bytes.Buffer

	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: utcTime})
	ist := time.FixedZone("IST", 5*3600+1800)
	r := slog.NewRecord(time.Date(2026, 10, 6, 11, 24, 14, 651_900_000, ist), slog.LevelInfo, "[User]", 0)
	r.AddAttrs(slog.String("call_id", "c1"))

	assert.NoError(t, h.Handle(t.Context(), r))
	assert.Equal(t, `time="2026-10-06 05:54:14.651" level=INFO msg=[User] call_id=c1`+"\n", buf.String())
}
