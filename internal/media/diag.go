package media

import (
	"log/slog"
	"sync"
)

// logFrameSizeOnce logs one distinct (codec-type, payload-size) combination
// the first time it's seen, at Warn (visible without enabling Debug) --
// diagnostic for tracking down which wire shapes a misbehaving codec/transport
// path actually produces, without flooding the log at whatever fraction of
// frames come back the wrong size. kind is the AudioSocket frame Kind byte or
// the RTP PayloadType, whichever transport this is.
var (
	seenFrameSizesMu sync.Mutex
	seenFrameSizes   = map[[2]int]struct{}{}
)

func logFrameSizeOnce(callID string, kind, size int) {
	key := [2]int{kind, size}

	seenFrameSizesMu.Lock()
	_, seen := seenFrameSizes[key]
	if !seen {
		seenFrameSizes[key] = struct{}{}
	}
	seenFrameSizesMu.Unlock()

	if !seen {
		slog.Warn("unexpected frame size (first occurrence of this kind+size)",
			"call_id", callID, "kind", kind, "size", size, "want", FrameSize)
	}
}
