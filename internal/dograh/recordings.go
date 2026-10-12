package dograh

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nitesh/vaani/internal/ai/agent"
)

// Dograh's workflow recordings (workflow_recordings, api/services/pipecat/
// recording_audio_cache.py): an organization's uploaded audio files, played
// instead of TTS for a greeting, a transition, a tool's message, or an LLM
// reply that asks for one. Each is downloaded from MinIO, converted by
// ffmpeg to 16kHz mono PCM, trimmed of leading/trailing silence, and cached
// in memory and on disk -- as Dograh does.

// recordingInstructions is Dograh's RECORDING_RESPONSE_MODE_INSTRUCTIONS
// (pipecat_engine_context_composer.py), word for word: appended to a node's
// prompt when it mentions "RECORDING_ID:" and the organization has
// recordings.
const recordingInstructions = `RESPONSE MODE INSTRUCTIONS - MANDATORY FORMAT:
Every response you generate MUST begin with excatcly one response mode indicator.
You have two modes for responding:

1. DYNAMIC SPEECH (▸): Generate text that will be converted to speech by TTS.
   Format: ▸ followed by a space and your full spoken response. Nothing else.
   Example: ▸ Hello! How can I help you today?

2. PRE-RECORDED AUDIO (●): Play a pre-recorded audio message.
   Format: ● followed by a space followed by recording_id followed by provided transcript. Nothing else.
   Example: ● rec_greeting_01 [ Provided Transcript ]

RULES:
- Your response MUST start with either ▸ or ● as the very first character.
- For ▸ (dynamic speech): Follow with a space and your response to be generated using TTS engine. Dont mix with ●
- For ● (pre-recorded audio): Follow with a space and recording_id of the audio clip with its transcript. Dont mix with ▸
- Use ● when a pre-recorded message matches the situation well.
- Use ▸ when you need to generate a dynamic, contextual response.
- *NEVER* mix modes in a single response, since we rely on the markers to decide whether to play using TTS or Pre-recorded audio.`

// promptRecordingRef is what makes a node prompt use recording response mode.
const promptRecordingRef = "RECORDING_ID:"

const (
	// clipLoadTimeout bounds one recording's download + conversion. Detached
	// from the call: a load the call can't wait for still fills the cache.
	clipLoadTimeout = 30 * time.Second
	// maxRecordingBytes caps a downloaded recording file.
	maxRecordingBytes = 50 << 20
	// clipSampleRate is the pipeline's rate: 16kHz s16le (invariant #8).
	clipSampleRate = 16000
	// speakingThreshold is pipecat's SPEAKING_THRESHOLD, the amplitude
	// Dograh's _trim_silence treats as sound.
	speakingThreshold = 20
)

// recordingRef is a recording's primary key as the workflow stores it:
// Dograh writes it as a number or a numeric string (and reads it with int()).
type recordingRef int64

func (r *recordingRef) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	switch x := v.(type) {
	case float64:
		*r = recordingRef(x)
	case string:
		if x == "" {
			*r = 0
			return nil
		}

		n, err := strconv.ParseInt(x, 10, 64)
		if err != nil {
			return fmt.Errorf("recording id %q: %w", x, err)
		}

		*r = recordingRef(n)
	default:
		*r = 0
	}

	return nil
}

// recordingRow is one active workflow_recordings row.
type recordingRow struct {
	ID          int64
	RecordingID string
	Transcript  string
	StorageKey  string
	Backend     string
}

// clipSet is the call's recordings, ready to play: by primary key (the
// greeting, transitions and tools pick one from a dropdown) and by
// recording_id (what an LLM reply names). HasAny is Dograh's
// has_active_recordings, whether or not every clip could be loaded.
type clipSet struct {
	byPK   map[int64]*agent.Clip
	byID   map[string]*agent.Clip
	HasAny bool
}

// clipCache holds converted recordings by storage key (a new upload gets a
// new key, so an edited recording is never served stale), in memory and on
// disk across restarts. One per process; loads are shared by concurrent
// calls.
type clipCache struct {
	storage *Storage
	dir     string

	mu       sync.Mutex
	pcm      map[string][]byte
	inflight map[string]chan struct{}
}

// UseStorage lets the store play Dograh's recordings, read from storage
// (Dograh's MinIO).
func (s *Store) UseStorage(storage *Storage) {
	s.clips = &clipCache{
		storage:  storage,
		dir:      filepath.Join(os.TempDir(), "vaani-recordings"),
		pcm:      map[string][]byte{},
		inflight: map[string]chan struct{}{},
	}
}

// loadClips reads organization orgID's active recordings and loads the ones
// this workflow needs: pks (referenced by the greeting, a transition or a
// tool), and every one if allByID (a node prompt names recordings). Clips
// that can't be loaded in time are left out, with a warning.
func (s *Store) loadClips(ctx context.Context, orgID *int64, pks map[int64]bool, allByID bool) (clipSet, []string) {
	set := clipSet{byPK: map[int64]*agent.Clip{}, byID: map[string]*agent.Clip{}}

	if orgID == nil || (len(pks) == 0 && !allByID) {
		return set, nil
	}

	rows, err := s.recordings(ctx, *orgID)
	if err != nil {
		return set, []string{"recordings unavailable: " + err.Error()}
	}

	set.HasAny = len(rows) > 0

	var warnings []string

	if s.clips == nil {
		if len(pks) > 0 || (allByID && set.HasAny) {
			warnings = append(warnings, "recordings unavailable: no MinIO configured (MINIO_ENDPOINT)")
		}

		return set, warnings
	}

	found := map[int64]bool{}

	for _, r := range rows {
		if !pks[r.ID] && !allByID {
			continue
		}

		found[r.ID] = true

		pcm, err := s.clips.get(ctx, r)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("recording %s (%d) unavailable: %v", r.RecordingID, r.ID, err))
			continue
		}

		clip := &agent.Clip{ID: r.RecordingID, PCM: pcm, Transcript: r.Transcript}
		set.byPK[r.ID] = clip
		set.byID[r.RecordingID] = clip
	}

	for pk := range pks {
		if !found[pk] {
			warnings = append(warnings, fmt.Sprintf("recording %d not found or not active", pk))
		}
	}

	return set, warnings
}

func (s *Store) recordings(ctx context.Context, orgID int64) ([]recordingRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, recording_id, transcript, storage_key, storage_backend::text
		FROM workflow_recordings
		WHERE organization_id = $1 AND is_active`, orgID)
	if err != nil {
		return nil, fmt.Errorf("query recordings: %w", err)
	}
	defer rows.Close()

	var out []recordingRow

	for rows.Next() {
		var r recordingRow
		if err := rows.Scan(&r.ID, &r.RecordingID, &r.Transcript, &r.StorageKey, &r.Backend); err != nil {
			return nil, fmt.Errorf("read recording: %w", err)
		}

		out = append(out, r)
	}

	return out, rows.Err()
}

// get returns r's converted PCM: from memory, else disk, else downloaded and
// converted -- once per storage key however many calls ask at the same
// time, on its own clipLoadTimeout, so a load this call gives up on (ctx)
// still finishes and caches for the next.
func (c *clipCache) get(ctx context.Context, r recordingRow) ([]byte, error) {
	if r.Backend != "minio" {
		return nil, fmt.Errorf("stored in %q; only minio is supported", r.Backend)
	}

	for {
		c.mu.Lock()

		if pcm, ok := c.pcm[r.StorageKey]; ok {
			c.mu.Unlock()
			return pcm, nil
		}

		wait, loading := c.inflight[r.StorageKey]
		if !loading {
			wait = make(chan struct{})
			c.inflight[r.StorageKey] = wait

			go c.load(r, wait)
		}

		c.mu.Unlock()

		select {
		case <-wait:
		case <-ctx.Done():
			return nil, fmt.Errorf("still loading: %w", ctx.Err())
		}

		c.mu.Lock()
		_, ok := c.pcm[r.StorageKey]
		c.mu.Unlock()

		if !ok {
			return nil, errors.New("load failed (see recording.load_failed)")
		}
	}
}

// load fills the cache for r and closes done.
func (c *clipCache) load(r recordingRow, done chan struct{}) {
	pcm, err := c.fetch(r)

	c.mu.Lock()
	if err == nil {
		c.pcm[r.StorageKey] = pcm
	}

	delete(c.inflight, r.StorageKey)
	c.mu.Unlock()
	close(done)

	if err != nil {
		slog.Warn("dograh: loading a recording failed", "component", "dograh", "event", "recording.load_failed",
			"recording_id", r.RecordingID, "storage_key", r.StorageKey, "error", err)
	}
}

func (c *clipCache) fetch(r recordingRow) ([]byte, error) {
	sum := sha256.Sum256([]byte(r.StorageKey))
	cached := filepath.Join(c.dir, hex.EncodeToString(sum[:16])+".pcm")

	if pcm, err := os.ReadFile(cached); err == nil && len(pcm) > 0 {
		return pcm, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), clipLoadTimeout)
	defer cancel()

	file, err := c.storage.Get(ctx, r.StorageKey, maxRecordingBytes)
	if err != nil {
		return nil, err
	}

	pcm, err := convertAudio(ctx, file, filepath.Ext(r.StorageKey))
	if err != nil {
		return nil, err
	}

	pcm = trimSilence(pcm)

	if err := os.MkdirAll(c.dir, 0o755); err == nil {
		if err := os.WriteFile(cached, pcm, 0o644); err != nil {
			slog.Warn("dograh: caching a recording on disk failed", "component", "dograh", "error", err)
		}
	}

	return pcm, nil
}

// convertAudio is Dograh's convert_audio_file: ffmpeg turns any audio file
// into raw 16kHz mono s16le. The file goes through a temp file with its own
// extension, not a pipe, as some containers need a seekable input.
func convertAudio(ctx context.Context, file []byte, ext string) ([]byte, error) {
	ffmpeg, err := exec.LookPath("ffmpeg")
	if err != nil {
		return nil, errors.New("ffmpeg not found on PATH")
	}

	tmp, err := os.CreateTemp("", "vaani-rec-*"+ext)
	if err != nil {
		return nil, fmt.Errorf("temp file: %w", err)
	}

	defer func() { _ = os.Remove(tmp.Name()) }()

	if _, err := tmp.Write(file); err != nil {
		_ = tmp.Close()
		return nil, fmt.Errorf("temp file: %w", err)
	}

	_ = tmp.Close()

	var out, stderr bytes.Buffer

	cmd := exec.CommandContext(ctx, ffmpeg, "-i", tmp.Name(), "-f", "s16le", "-acodec", "pcm_s16le",
		"-ac", "1", "-ar", strconv.Itoa(clipSampleRate), "-loglevel", "error", "pipe:1")
	cmd.Stdout, cmd.Stderr = &out, &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("ffmpeg: %w: %s", err, strings.TrimSpace(stderr.String()))
	}

	if out.Len() == 0 {
		return nil, errors.New("ffmpeg produced no audio")
	}

	return out.Bytes(), nil
}

// trimSilence is Dograh's _trim_silence: drops leading and trailing 10ms
// frames whose loudest sample doesn't exceed speakingThreshold. All-silent
// audio is returned as-is.
func trimSilence(pcm []byte) []byte {
	const frameBytes = clipSampleRate / 100 * 2 // 10ms of s16le

	frames := len(pcm) / frameBytes

	loud := func(i int) bool {
		f := pcm[i*frameBytes : (i+1)*frameBytes]
		for j := 0; j+1 < len(f); j += 2 {
			v := int(int16(binary.LittleEndian.Uint16(f[j:])))
			if v > speakingThreshold || v < -speakingThreshold {
				return true
			}
		}

		return false
	}

	first := -1

	for i := 0; i < frames; i++ {
		if loud(i) {
			first = i
			break
		}
	}

	if first < 0 {
		return pcm
	}

	last := first

	for i := frames - 1; i > first; i-- {
		if loud(i) {
			last = i
			break
		}
	}

	return pcm[first*frameBytes : (last+1)*frameBytes]
}
