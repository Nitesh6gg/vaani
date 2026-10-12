package agent

import (
	"context"
	"log/slog"
	"regexp"
	"strings"

	"github.com/nitesh/vaani/internal/ai/tts"
)

// Dograh's recording response mode markers (pipecat_engine_context_composer
// .py): a reply starting with RecordingMarker plays a recording, one starting
// with TTSMarker is spoken.
const (
	RecordingMarker = "●"
	TTSMarker       = "▸"
)

type routerMode int

const (
	modeDetecting routerMode = iota
	modeTTS
	modeRecording
)

// clipRouter is Dograh's RecordingRouterProcessor for one LLM reply (one
// Stream round): it holds the reply back until its marker arrives, then
// either passes the text after "▸" on to TTS or, after "●", picks out the
// recording_id that follows -- as soon as a space ends it, so playback starts
// without waiting for the transcript the LLM writes after it. Modes never
// mix: whatever follows a second marker is dropped. A reply with no marker
// at all is spoken as-is.
type clipRouter struct {
	mode    routerMode
	buf     string // detecting: the reply so far; recording: the text after "●"
	stopped bool   // tts: a "●" came after; recording: the id was already picked
}

// feed takes one LLM token and returns the text to send to TTS, and the
// recording to play (at most once per reply).
func (r *clipRouter) feed(tok string) (speak, recordingID string) {
	switch r.mode {
	case modeTTS:
		if r.stopped {
			return "", ""
		}

		if i := strings.Index(tok, RecordingMarker); i >= 0 {
			r.stopped = true
			return tok[:i], ""
		}

		return tok, ""
	case modeRecording:
		if r.stopped {
			return "", ""
		}

		if i := strings.Index(tok, TTSMarker); i >= 0 {
			tok = tok[:i]
		}

		r.buf += tok

		// The id is complete once whitespace follows it.
		if rest := strings.TrimLeft(r.buf, " \t\n"); strings.ContainsAny(rest, " \t\n") {
			r.stopped = true
			return "", strings.Fields(rest)[0]
		}

		return "", ""
	}

	r.buf += tok

	// Whichever marker comes first decides (Dograh checks "●" first even
	// when "▸" precedes it in the same chunk).
	rec, say := strings.Index(r.buf, RecordingMarker), strings.Index(r.buf, TTSMarker)

	switch {
	case rec >= 0 && (say < 0 || rec < say):
		rest := r.buf[rec+len(RecordingMarker):]
		r.mode, r.buf = modeRecording, ""

		return r.feed(rest)
	case say >= 0:
		rest := strings.TrimPrefix(r.buf[say+len(TTSMarker):], " ")
		r.mode, r.buf = modeTTS, ""

		return r.feed(rest)
	}

	return "", ""
}

// speakRest turns a "●" reply whose recording isn't available into a spoken
// one: what the LLM had written after the id when it was picked is returned
// for TTS, and the rest of the reply follows as ordinary speech. The LLM
// writes the recording's transcript there, so the caller hears what the
// recording would have said instead of silence (Dograh plays nothing).
func (r *clipRouter) speakRest() string {
	rest := strings.TrimLeft(r.buf, " \t\n")
	if f := strings.Fields(rest); len(f) > 0 {
		rest = strings.TrimLeft(strings.TrimPrefix(rest, f[0]), " \t\n")
	}

	// Modes never mix: a second "●" ends the speech (and anything after it
	// that's still to come).
	stopped := false
	if i := strings.Index(rest, RecordingMarker); i >= 0 {
		rest, stopped = rest[:i], true
	}

	r.mode, r.buf, r.stopped = modeTTS, "", stopped

	return rest
}

// flush ends the reply: a recording whose id was never followed by a space
// plays now; a reply that never had a marker is spoken as-is.
func (r *clipRouter) flush() (speak, recordingID string) {
	switch r.mode {
	case modeRecording:
		if f := strings.Fields(r.buf); !r.stopped && len(f) > 0 {
			r.stopped = true
			return "", f[0]
		}
	case modeDetecting:
		s := r.buf
		r.buf = ""

		return s, ""
	}

	return "", ""
}

// recordingRefPattern is the "● <recording_id> " a recorded reply carries in
// the LLM's history (see playRecording).
var recordingRefPattern = regexp.MustCompile(`●\s*(\S+)\s*`)

// stripRecordingRefs is text without the "● <recording_id> " references of
// recorded replies, and those ids in order. The LLM's own history keeps them
// (it needs the format it's asked to follow); the call's log and transcript
// show only what was said.
func stripRecordingRefs(text string) (spoken string, ids []string) {
	for _, m := range recordingRefPattern.FindAllStringSubmatch(text, -1) {
		ids = append(ids, m[1])
	}

	return strings.TrimSpace(recordingRefPattern.ReplaceAllString(text, "")), ids
}

// playClip queues clip in place of synthesized speech, after whatever this
// turn already sent to TTS; text is what the turn records for it (history,
// call log). Unlike sendToTTS it costs no TTS characters.
func (h *Handler) playClip(ctx context.Context, ttsClient tts.Client, clip *Clip, text string, gen uint64) {
	if ctx.Err() != nil {
		return
	}

	if err := ttsClient.Play(clip.PCM, text, gen); err != nil {
		h.cfg.Sink.Error("tts_speak")

		if ctx.Err() == nil {
			slog.Warn("recorded clip failed", "call_id", h.callID, "gen", gen, "event", "tts.error",
				"recording_id", clip.ID, "error", err)
		}

		return
	}

	slog.Debug("recorded clip queued", "call_id", h.callID, "gen", gen, "event", "tts.clip",
		"recording_id", clip.ID, "audio_ms", len(clip.PCM)/32)
}

// playRecording plays the recording the LLM asked for with "● <id>": what
// the turn records is the marker, id and transcript, so the LLM's history
// shows the reply format it's asked to keep, as Dograh's context does. It
// reports whether the recording exists; if not, nothing was played and the
// caller falls back to speaking the text the LLM wrote (clipRouter.speakRest).
func (h *Handler) playRecording(ctx context.Context, ttsClient tts.Client, node *Node, id string, gen uint64) bool {
	clip := node.Clips[id]
	if clip == nil {
		slog.Warn("llm asked for a recording that isn't available; speaking the text it wrote instead", "call_id", h.callID,
			"gen", gen, "event", "tts.clip_missing", "recording_id", id)

		return false
	}

	h.playClip(ctx, ttsClient, clip, RecordingMarker+" "+clip.ID+" "+clip.Transcript+" ", gen)

	return true
}
