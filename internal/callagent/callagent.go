// Package callagent builds the Dograh agent for one call -- workflow, STT,
// TTS, LLM, barge-in detector -- and records the call in Dograh's call history
// once it ends. It doesn't know the switch: the Asterisk (internal/ari) and
// FreeSWITCH (internal/freeswitch) call managers both use it, passing in how
// to hang the call up and, where supported, transfer it.
package callagent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sync"
	"time"

	"github.com/nitesh/vaani/assets"
	"github.com/nitesh/vaani/internal/ai/agent"
	"github.com/nitesh/vaani/internal/ai/agent/tenvad"
	"github.com/nitesh/vaani/internal/ai/llm"
	"github.com/nitesh/vaani/internal/ai/stt"
	"github.com/nitesh/vaani/internal/ai/tts"
	"github.com/nitesh/vaani/internal/config"
	"github.com/nitesh/vaani/internal/dograh"
	"github.com/nitesh/vaani/internal/media"
	"github.com/nitesh/vaani/internal/metrics"
)

// Builder builds each call's agent. One per process, shared by all calls.
type Builder struct {
	cfg config.Config
	// store reads the agent's workflow from Dograh's database and records
	// each call there; nil when DOGRAH_DB_URL isn't set. storage is Dograh's
	// MinIO for recordings and transcripts; nil = no uploads.
	store   *dograh.Store
	storage *dograh.Storage
	// hold/beep are the transfer sounds (see assets), 16kHz PCM.
	hold []byte
	beep []byte

	// runs counts calls whose Dograh run is still being written, so
	// shutdown can let them finish. procCtx is the process's context,
	// cancelled when Vaani is stopping -- not any one call's.
	runs    sync.WaitGroup
	procCtx context.Context
}

// New returns a Builder. store and storage may be nil.
func New(cfg config.Config, store *dograh.Store, storage *dograh.Storage) *Builder {
	b := &Builder{cfg: cfg, store: store, storage: storage, procCtx: context.Background()}

	// Embedded files: an error here means a broken build, but a transfer
	// still works without them (the caller just hears silence), so warn.
	var err error
	if b.hold, err = assets.TransferHoldRing(); err != nil {
		slog.Warn("transfer hold audio unavailable; callers will hear silence while a transfer rings", "error", err)
	}

	if b.beep, err = assets.Beep(); err != nil {
		slog.Warn("transfer beep unavailable", "error", err)
	}

	return b
}

// SetProcessContext sets the process's context (see procCtx). Call it before
// the first call starts.
func (b *Builder) SetProcessContext(ctx context.Context) { b.procCtx = ctx }

// Wait blocks until every call's Dograh run has been written.
func (b *Builder) Wait() { b.runs.Wait() }

// Call identifies one call to the agent and to Dograh.
type Call struct {
	ID           string
	CallerNumber string
	CalledNumber string
}

// Hooks are what the agent can do to the call on the switch.
type Hooks struct {
	// Hangup hangs the caller up; the switch's hangup then ends the call.
	Hangup func()
	// Transfer dials destination and returns the function that hands the
	// caller over (see agent.Config.Transfer); nil = the switch can't
	// transfer, and the agent tells the caller so.
	Transfer func(ctx context.Context, destination string, timeout time.Duration) (connect func() error, err error)
	// Interrupted, if set, is called when the caller starts talking over the
	// agent and its reply pauses (agent.Sink.InterruptionPaused), on the
	// agent's own goroutine: it must not block.
	Interrupted func()
}

// interruptSink calls Hooks.Interrupted when a reply pauses for the caller.
type interruptSink struct {
	agent.NoopSink
	fn func()
}

func (s interruptSink) InterruptionPaused() { s.fn() }

// Handler builds call c's media Handler: the Dograh agent when APP_MODE=agent
// (nil -- the transport's loopback -- otherwise). ctx scopes the agent: it
// ends the agent when cancelled.
func (b *Builder) Handler(ctx context.Context, c Call, hooks Hooks) media.Handler {
	if b.cfg.TestSilentHandler {
		slog.Warn("TEST_SILENT_HANDLER=1: this call will carry no audio", "call_id", c.ID)
		return media.SilentHandler{}
	}

	if b.cfg.AppMode != "agent" {
		return nil
	}

	return b.newAgentHandler(ctx, c, hooks)
}

// connectRetryAttempts/connectRetryBaseDelay bound the retry budget for a
// call's initial STT connection and its first-turn TTS connection: a handful
// of quick attempts, not the standing background retry loop that a sibling
// project's own incident history (D:\go-agent-worker's ADR-011) warns
// against for a connection that drops mid-call -- this only ever runs at
// connection-open time and always terminates, in success or exhaustion.
const (
	connectRetryAttempts  = 3
	connectRetryBaseDelay = 500 * time.Millisecond
)

// dialWithRetry calls dial up to connectRetryAttempts times with exponential
// backoff, stopping early if ctx is done. Used for both the call-scoped STT
// connection and the lazily-opened, call-scoped TTS connection (see
// tts.Client's doc comment) -- both are opened once per call, so a small
// bounded retry here is worth it even though the provider itself offers no
// retry of its own.
func dialWithRetry[T any](ctx context.Context, callID, what string, dial func() (T, error)) (T, error) {
	var (
		v     T
		err   error
		delay = connectRetryBaseDelay
	)

	for attempt := 1; attempt <= connectRetryAttempts; attempt++ {
		v, err = dial()
		if err == nil {
			return v, nil
		}

		if attempt == connectRetryAttempts {
			break
		}

		slog.Warn("agent: connect failed, retrying", "call_id", callID, "event", "provider.retry", "what", what, "attempt", attempt, "error", err)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		}

		delay *= 2
	}

	return v, fmt.Errorf("%s: %w", what, err)
}

// loadWorkflow reads Dograh workflow DOGRAH_WORKFLOW_ID for call c, fresh on
// every call so a publish in Dograh's editor applies to the next call.
func (b *Builder) loadWorkflow(ctx context.Context, c Call) (*dograh.Workflow, error) {
	if b.store == nil {
		return nil, errors.New("no dograh database (DOGRAH_DB_URL)")
	}

	qctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	wf, warnings, err := b.store.Workflow(qctx, b.cfg.DograhWorkflowID, map[string]any{
		"caller_number": c.CallerNumber,
		"called_number": c.CalledNumber,
	})
	if err != nil {
		return nil, err
	}

	for _, w := range warnings {
		slog.Warn("agent: workflow: "+w, "call_id", c.ID, "component", "dograh", "event", "workflow.warning", "workflow_id", b.cfg.DograhWorkflowID)
	}

	opening := "llm"
	if wf.Start.Greeting != "" {
		opening = "greeting"
	}

	slog.Info("agent workflow loaded", "call_id", c.ID, "component", "dograh", "event", "workflow.loaded", "workflow_id", b.cfg.DograhWorkflowID,
		"start_node", wf.Start.Name, "opening", opening, "start_tools", toolNames(wf.Start),
		"start_allow_interrupt", wf.Start.AllowInterrupt,
		"start_interrupt", agent.InterruptMode(b.cfg.BargeInEnabled, wf.Start),
		"idle_timeout_s", wf.IdleTimeout.Seconds(), "max_duration_s", wf.MaxDuration.Seconds())

	// Never the keys.
	s := wf.Services
	slog.Info("agent models", "call_id", c.ID, "component", "agent", "event", "agent.configured",
		"llm", s.LLM.Provider+"/"+s.LLM.Model, "llm_url", s.LLM.BaseURL,
		"stt", "sarvam/"+s.STT.Model, "stt_language", s.STT.Language,
		"tts", "sarvam/"+s.TTS.Model, "tts_voice", s.TTS.Voice, "tts_language", s.TTS.Language)

	return wf, nil
}

// toolNames lists the functions the LLM is offered at n (tools, then edges).
func toolNames(n *agent.Node) []string {
	var names []string

	for _, t := range n.Tools {
		names = append(names, t.Def.Name)
	}

	for _, e := range n.Edges {
		names = append(names, e.Def.Name+"->"+e.To.Name)
	}

	return names
}

// sinks is where the agent's events go: metrics, the call's own log, and the
// switch's Interrupted hook if it has one.
func sinks(callLog *agent.CallLog, hooks Hooks) agent.Sink {
	if hooks.Interrupted == nil {
		return agent.Sinks(metrics.AgentSink{}, callLog)
	}

	return agent.Sinks(metrics.AgentSink{}, callLog, interruptSink{fn: hooks.Interrupted})
}

// newAgentHandler wires internal/ai/{stt,tts,llm,agent} together for one
// call: the models from Dograh (see dograh.Services), barge-in from b.cfg's
// BARGE_IN_* settings. If the STT connection can't
// be established even after dialWithRetry's budget, the call falls back to
// LoopbackHandler (nil) rather than failing the call outright -- logged
// loudly since a caller in agent mode getting loopback behavior instead is a
// real, visible degradation, not a silent one.
func (b *Builder) newAgentHandler(ctx context.Context, c Call, hooks Hooks) media.Handler {
	callID := c.ID

	// Without its workflow the agent has no prompt and nothing to say: end
	// the call rather than run a blank agent. Silence until the hangup lands.
	wf, err := b.loadWorkflow(ctx, c)
	if err != nil {
		slog.Error("agent: loading the dograh workflow failed; hanging up", "call_id", callID, "component", "dograh", "event", "workflow.load_failed",
			"workflow_id", b.cfg.DograhWorkflowID, "error", err)

		go hooks.Hangup()

		return media.SilentHandler{}
	}

	svc := wf.Services

	sttClient, err := dialWithRetry(ctx, callID, "stt connect", func() (stt.Client, error) {
		return stt.NewSarvamClient(ctx, callID, stt.Config{
			APIKey:   svc.STT.APIKey,
			Model:    svc.STT.Model,
			Language: svc.STT.Language,
			// Only when a flush can actually be sent: early finalization on
			// and TEN VAD (which tells when the caller stopped) selected.
			// Otherwise the connection stays exactly as before.
			FlushSignal: b.cfg.STTFlushAfter > 0 && b.cfg.VadMode == "ten",
		})
	})
	if err != nil {
		slog.Error("agent: STT connect failed after retries, falling back to loopback", "call_id", callID, "component", "stt", "event", "stt.connect_failed", "error", err)
		return nil
	}

	// VAD_MODE=ten always runs the real TEN VAD -- even with barge-in
	// disabled, where it observes (speech started/ended logs) without ever
	// cutting. BARGE_IN_ENABLED then only decides whether the detector's
	// verdict is allowed to cut playback (see agent.Config.BargeInObserveOnly).
	bargeIn := b.newBargeInDetector(callID)
	callLog := &agent.CallLog{}

	h := agent.NewHandler(ctx, callID, agent.Config{
		STT: sttClient,
		NewTTS: func() (tts.Client, error) {
			return dialWithRetry(ctx, callID, "tts connect", func() (tts.Client, error) {
				return tts.NewSarvamClient(ctx, callID, tts.Config{
					APIKey:   svc.TTS.APIKey,
					Voice:    svc.TTS.Voice,
					Model:    svc.TTS.Model,
					Language: svc.TTS.Language,
				})
			})
		},
		LLM:         llm.NewClient(svc.LLM.BaseURL, svc.LLM.APIKey, svc.LLM.Model),
		Start:       wf.Start,
		IdleTimeout: wf.IdleTimeout,
		MaxDuration: wf.MaxDuration,
		// Hanging up the caller's channel ends the call through the switch's
		// own hangup path (StasisEnd on Asterisk, CHANNEL_HANGUP on FreeSWITCH).
		Hangup:             hooks.Hangup,
		Transfer:           hooks.Transfer,
		HoldAudio:          b.hold,
		BeepAudio:          b.beep,
		BargeIn:            bargeIn,
		BargeInObserveOnly: !b.cfg.BargeInEnabled,
		BargeInGuard:       b.cfg.BargeInGuard,
		BargeInMinSpeech:   b.cfg.BargeInMinSpeech,
		STTFlushAfter:      b.cfg.STTFlushAfter,
		PostCutSilence:     b.cfg.PostCutSilence,
		AckFilter:          b.cfg.BargeInAckFilter,
		AckEndMargin:       b.cfg.AckEndMargin,
		Sink:               sinks(callLog, hooks),
		Log:                callLog,
	})

	rec := media.NewMixRecorder(h)

	// recordRun logs about this call after its teardown: keep its trace_id
	// (and run_id) on those lines until it's done.
	config.HoldCallTrace(callID)

	b.runs.Go(func() { b.recordRun(c, wf, callLog, rec, h) })

	return rec
}

// recordRun records call c in Dograh's call history (workflow_runs), as
// Dograh does for its own calls: the run is created now and completed once
// the agent has left the call (hangup or transfer) -- with the conversation,
// the variables extracted, how it ended, and the recording and transcript
// uploaded to Dograh's MinIO. Failures are logged; the call itself is never
// affected.
// Run on b.runs (shutdown waits for it).
func (b *Builder) recordRun(c Call, wf *dograh.Workflow, callLog *agent.CallLog, rec *media.MixRecorder, h *agent.Handler) {
	defer config.UnregisterCallTrace(c.ID) // HoldCallTrace in newAgentHandler

	started := time.Now()

	// Not the call's context: the record is completed after the call ends.
	// StartRun bounds (and retries) itself.
	runID, err := b.store.StartRun(b.procCtx, wf, dograh.RunCall{
		CallID: c.ID, CallerNumber: c.CallerNumber, CalledNumber: c.CalledNumber,
	})
	if err != nil {
		slog.Error("dograh run: not created; this call won't appear in Dograh", "call_id", c.ID,
			"component", "dograh", "event", "run.create_failed", "error", err)
	} else {
		config.SetCallRunID(c.ID, runID) // every later line of this call carries run_id
		slog.Info("dograh run created", "call_id", c.ID, "component", "dograh", "event", "run.created",
			"workflow_id", wf.ID)
	}

	<-h.Done()

	duration := time.Since(started)

	// Dograh extracts the last node's variables as the call ends, after any
	// still running from earlier nodes (each bounded by its own timeout).
	// On the process's context: when Vaani is stopping (a deploy), the
	// extraction is skipped so the run's completion fits in the shutdown
	// window -- a call left at 'running' in Dograh is worse than one without
	// its last node's variables.
	h.FinishExtraction(b.procCtx)

	summary := callLog.Summary()
	recording := rec.WAV()

	if rec.Truncated() {
		metrics.RecordingTruncated.Inc()
		slog.Warn("call recording hit its size cap; the end of the call is missing from it",
			"call_id", c.ID, "component", "media", "event", "recording.truncated", "recording_bytes", len(recording))
	}

	reason := summary.EndReason
	if reason == "" {
		reason = agent.EndReasonUserHangup
	}

	logCallSummary(c.ID, duration, reason, summary, len(recording))

	if runID == 0 {
		return // not created; already logged
	}

	// FinishRun bounds each of its steps itself.
	err = b.store.FinishRun(context.Background(), wf, runID, dograh.RunEnd{
		Summary: summary, Duration: duration, Recording: recording,
	}, b.storage)
	if err != nil {
		slog.Error("dograh run: completing it failed", "call_id", c.ID, "component", "dograh",
			"event", "run.complete_failed", "error", err)

		return
	}

	slog.Info("dograh run completed", "call_id", c.ID, "component", "dograh", "event", "run.completed",
		"disposition", reason, "events", len(summary.Events), "variables", len(summary.Extracted),
		"recording_bytes", len(recording), "minio", b.storage != nil)
}

// logCallSummary writes the call's one summary line: what to chart and alert
// on (duration, turns, reply latency, interruptions, errors, outcome).
func logCallSummary(callID string, duration time.Duration, disposition string, s agent.CallSummary, recordingBytes int) {
	args := []any{"call_id", callID, "component", "agent", "event", "call.summary",
		"duration_s", int(math.Round(duration.Seconds())), "disposition", disposition,
		"user_turns", s.UserTurns, "agent_replies", s.AgentReplies,
		"bargeins", s.BargeIns, "pauses", s.Pauses, "false_interruptions", s.FalseInterruptions,
		"held_speech", s.HeldSpeech, "held_delivered", s.HeldDelivered,
		"nodes", s.NodesVisited, "variables", len(s.Extracted), "recording_bytes", recordingBytes}

	if p50, p95, ok := percentiles(s.ReplyLatenciesMS); ok {
		args = append(args, "reply_latency_p50_ms", p50, "reply_latency_p95_ms", p95)
	}

	if len(s.Errors) > 0 {
		args = append(args, "errors", s.Errors)
	}

	slog.Info("call summary", args...)
}

// percentiles returns the 50th and 95th percentile (nearest rank) of v.
func percentiles(v []int64) (p50, p95 int64, ok bool) {
	if len(v) == 0 {
		return 0, 0, false
	}

	s := slices.Clone(v)
	slices.Sort(s)

	rank := func(p float64) int64 {
		i := int(math.Ceil(p*float64(len(s)))) - 1
		return s[max(0, min(i, len(s)-1))]
	}

	return rank(0.50), rank(0.95), true
}

// newBargeInDetector builds the caller-interrupt detector for one call.
// VAD_MODE=ten selects TEN VAD (a real neural VAD -- see
// internal/ai/agent/tenvad) and runs it regardless of BARGE_IN_ENABLED:
// disabled barge-in means observe-only, not a dead VAD. Energy mode keeps
// the old semantics -- disabled barge-in is a true no-op, since the RMS
// detector has no logging to offer. Anything TEN VAD needs (the native
// library, a Linux cgo build) missing is a loud warn plus fallback to the
// energy detector, never a failed call: a worse detector beats no detector.
func (b *Builder) newBargeInDetector(callID string) agent.BargeInDetector {
	if b.cfg.VadMode == "ten" {
		det, err := tenvad.NewBargeInDetector(callID, b.cfg.TenVadThreshold)
		if err != nil {
			slog.Warn("agent: TEN VAD unavailable, falling back to the energy detector", "component", "vad", "event", "vad.fallback",
				"call_id", callID, "error", err)
		} else {
			mode := "cutting"
			if !b.cfg.BargeInEnabled {
				mode = "observe-only (BARGE_IN_ENABLED=0)"
			}

			slog.Info("agent: barge-in detector: TEN VAD",
				"call_id", callID, "component", "vad", "event", "vad.configured", "threshold", b.cfg.TenVadThreshold,
				"ten_vad_version", tenvad.Version(), "mode", mode)

			return det
		}
	}

	if b.cfg.BargeInEnabled {
		return agent.NewEnergyDetector(b.cfg.BargeInRMSFloor)
	}

	return agent.NoopBargeInDetector{}
}
