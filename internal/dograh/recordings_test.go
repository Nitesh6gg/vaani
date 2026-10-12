package dograh

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/ai/agent"
)

func buildToolNoClips(r ToolRow) (agent.Tool, string, error) { return buildTool(r, clipSet{}) }

func TestRecordingRefAcceptsNumberOrString(t *testing.T) {
	for in, want := range map[string]recordingRef{`12`: 12, `"34"`: 34, `""`: 0, `null`: 0} {
		var r recordingRef
		require.NoError(t, json.Unmarshal([]byte(in), &r), in)
		assert.Equal(t, want, r, in)
	}

	var r recordingRef
	assert.Error(t, json.Unmarshal([]byte(`"abc"`), &r))
}

const recordingWorkflow = `{"nodes": [
	{"id": "s", "type": "startCall", "data": {"name": "Start", "is_start": true, "prompt": "Greet.",
		"greeting_type": "audio", "greeting_recording_id": "7"}},
	{"id": "m", "type": "agentNode", "data": {"name": "Main", "prompt": "Answer. RECORDING_ID: offer_01 for the offer."}},
	{"id": "e", "type": "endCall", "data": {"name": "End", "is_end": true, "prompt": "Bye."}}
], "edges": [
	{"source": "s", "target": "m", "data": {"label": "Go", "condition": "next", "transition_speech": "ignored",
		"transition_speech_type": "audio", "transition_speech_recording_id": 8}},
	{"source": "m", "target": "e", "data": {"label": "Done", "condition": "end", "transition_speech": "Thanks!"}}
]}`

func TestRecordingNeeds(t *testing.T) {
	wf, err := parseWorkflow([]byte(recordingWorkflow))
	require.NoError(t, err)

	tools := []ToolRow{
		{Name: "end", Category: CategoryEndCall, Definition: json.RawMessage(`{"config": {"messageType": "audio", "audioRecordingId": "9"}}`)},
		{Name: "api", Category: CategoryHTTPAPI, Definition: json.RawMessage(`{"config": {"url": "https://x", "customMessageType": "audio", "customMessageRecordingId": 10}}`)},
		{Name: "api2", Category: CategoryHTTPAPI, Definition: json.RawMessage(`{"config": {"url": "https://x", "messageType": "audio", "audioRecordingId": 11}}`)}, // http ignores messageType
	}

	pks, byID := wf.recordingNeeds(tools)
	assert.Equal(t, map[int64]bool{7: true, 8: true, 9: true, 10: true}, pks)
	assert.True(t, byID, "a node prompt names RECORDING_ID:")
}

func TestBuildWorkflowWithClips(t *testing.T) {
	wf, err := parseWorkflow([]byte(recordingWorkflow))
	require.NoError(t, err)

	greet := &agent.Clip{ID: "hello", PCM: []byte{1}, Transcript: "Namaste!"}
	move := &agent.Clip{ID: "moving", PCM: []byte{2}, Transcript: "Ek second."}
	offer := &agent.Clip{ID: "offer_01", PCM: []byte{3}, Transcript: "Our offer."}

	clips := clipSet{
		byPK:   map[int64]*agent.Clip{7: greet, 8: move, 12: offer},
		byID:   map[string]*agent.Clip{"hello": greet, "moving": move, "offer_01": offer},
		HasAny: true,
	}

	start, warnings, err := buildWorkflow(wf, nil, nil, clips, time.Now())
	require.NoError(t, err)
	assert.Empty(t, warnings)

	assert.Same(t, greet, start.GreetingClip)
	assert.Empty(t, start.Greeting)
	assert.Nil(t, start.Clips, "Start's prompt names no recording")

	require.Len(t, start.Edges, 1)
	assert.Same(t, move, start.Edges[0].SpeechClip)
	assert.Empty(t, start.Edges[0].Speech, "audio transition: its text isn't spoken")

	main := start.Edges[0].To
	assert.Equal(t, clips.byID, main.Clips)
	assert.True(t, strings.HasSuffix(main.Prompt, recordingInstructions), "recording instructions appended")

	require.Len(t, main.Edges, 1)
	assert.Equal(t, "Thanks!", main.Edges[0].Speech)

	// No recordings in the organization: no response mode, and the greeting
	// falls back to an LLM opening, as in Dograh.
	start, warnings, err = buildWorkflow(wf, nil, nil, clipSet{}, time.Now())
	require.NoError(t, err)
	assert.Nil(t, start.GreetingClip)
	assert.Nil(t, start.Edges[0].To.Clips)
	assert.NotContains(t, start.Edges[0].To.Prompt, "RESPONSE MODE")
	assert.Len(t, warnings, 2, "greeting and transition recordings unavailable")
}

func TestBuildToolMessageClip(t *testing.T) {
	clip := &agent.Clip{ID: "bye", PCM: []byte{1}, Transcript: "Dhanyawad!"}
	clips := clipSet{byPK: map[int64]*agent.Clip{9: clip, 10: clip}}

	end, warn, err := buildTool(ToolRow{Name: "end", Category: CategoryEndCall,
		Definition: json.RawMessage(`{"config": {"messageType": "audio", "audioRecordingId": "9", "customMessage": "x"}}`)}, clips)
	require.NoError(t, err)
	assert.Empty(t, warn)
	assert.Same(t, clip, end.MessageClip)
	assert.Empty(t, end.Message)

	api, _, err := buildTool(ToolRow{Name: "api", Category: CategoryHTTPAPI,
		Definition: json.RawMessage(`{"config": {"url": "https://x", "customMessageType": "audio", "customMessageRecordingId": 10, "customMessage": "x"}}`)}, clips)
	require.NoError(t, err)
	assert.Same(t, clip, api.MessageClip)
	assert.Empty(t, api.Message)

	_, warn, err = buildTool(ToolRow{Name: "end", Category: CategoryEndCall,
		Definition: json.RawMessage(`{"config": {"messageType": "audio", "audioRecordingId": 99}}`)}, clips)
	require.NoError(t, err)
	assert.Contains(t, warn, "recording unavailable")
}

func pcmOf(samples ...int16) []byte {
	b := make([]byte, 2*len(samples))
	for i, s := range samples {
		binary.LittleEndian.PutUint16(b[2*i:], uint16(s))
	}

	return b
}

func TestTrimSilence(t *testing.T) {
	const frame = 160 // 10ms at 16kHz, in samples

	quiet := make([]int16, frame) // all zero
	loud := make([]int16, frame)
	loud[50] = -500

	var samples []int16
	samples = append(samples, quiet...)
	samples = append(samples, loud...)
	samples = append(samples, quiet...) // between loud frames: kept
	samples = append(samples, loud...)
	samples = append(samples, quiet...)
	samples = append(samples, quiet...)
	samples = append(samples, 1, 2, 3) // partial frame: dropped

	got := trimSilence(pcmOf(samples...))
	assert.Equal(t, 3*frame*2, len(got), "loud, quiet, loud")
	assert.Equal(t, pcmOf(loud...), got[:frame*2])

	silent := pcmOf(quiet...)
	assert.Equal(t, silent, trimSilence(silent), "all silence: returned as-is")
}

// TestClipCacheDiskAndBackend: a clip cached on disk is served without
// MinIO or ffmpeg; one stored outside MinIO is refused.
func TestClipCacheDiskAndBackend(t *testing.T) {
	var s Store

	s.UseStorage(&Storage{Endpoint: "127.0.0.1:1", Bucket: "b"}) // unreachable: must not be needed
	s.clips.dir = t.TempDir()

	row := recordingRow{ID: 1, RecordingID: "hello", StorageKey: "recordings/1/hello.mp3", Backend: "minio"}

	sum := sha256.Sum256([]byte(row.StorageKey))
	require.NoError(t, os.WriteFile(filepath.Join(s.clips.dir, hex.EncodeToString(sum[:16])+".pcm"), []byte{1, 2, 3, 4}, 0o644))

	pcm, err := s.clips.get(t.Context(), row)
	require.NoError(t, err)
	assert.Equal(t, []byte{1, 2, 3, 4}, pcm)

	_, err = s.clips.get(t.Context(), recordingRow{StorageKey: "k", Backend: "s3"})
	assert.ErrorContains(t, err, "only minio")
}

// wavOf is a minimal 16-bit mono WAV file.
func wavOf(rate int, samples []int16) []byte {
	var b bytes.Buffer

	data := pcmOf(samples...)
	w := func(v any) { _ = binary.Write(&b, binary.LittleEndian, v) }

	b.WriteString("RIFF")
	w(uint32(36 + len(data)))
	b.WriteString("WAVEfmt ")
	w(uint32(16))
	w(uint16(1))
	w(uint16(1))
	w(uint32(rate))
	w(uint32(rate * 2))
	w(uint16(2))
	w(uint16(16))
	b.WriteString("data")
	w(uint32(len(data)))
	b.Write(data)

	return b.Bytes()
}

// TestClipCacheDownloadsAndConverts: the full path -- MinIO download, ffmpeg
// to 16kHz, silence trim, disk cache. Needs ffmpeg; skipped without it.
func TestClipCacheDownloadsAndConverts(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}

	// 0.5s of 440Hz at 8kHz, padded with 0.2s of silence each side.
	var samples []int16
	samples = append(samples, make([]int16, 1600)...)

	for i := 0; i < 4000; i++ {
		samples = append(samples, int16(8000*math.Sin(2*math.Pi*440*float64(i)/8000)))
	}

	samples = append(samples, make([]int16, 1600)...)
	file := wavOf(8000, samples)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/b/recordings/1/hello.wav" {
			http.NotFound(w, r)
			return
		}

		_, _ = w.Write(file)
	}))
	defer srv.Close()

	var s Store

	s.UseStorage(&Storage{Endpoint: strings.TrimPrefix(srv.URL, "http://"), Bucket: "b"})
	s.clips.dir = t.TempDir()

	pcm, err := s.clips.get(context.Background(), recordingRow{ID: 1, RecordingID: "hello",
		StorageKey: "recordings/1/hello.wav", Backend: "minio"})
	require.NoError(t, err)

	// ~0.5s at 16kHz once the silence is trimmed (resampling smears the edges a little).
	assert.InDelta(t, 16000, len(pcm), 1000)

	entries, _ := os.ReadDir(s.clips.dir)
	assert.Len(t, entries, 1, "cached on disk")
}
