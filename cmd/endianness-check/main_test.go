package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/media"
)

// swapBytes returns a copy of b with every adjacent byte pair swapped --
// simulating what a little-endian-encoded waveform looks like on the wire when
// the true encoding was actually the other byte order, or vice versa.
func swapBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)

	for i := 0; i+1 < len(out); i += 2 {
		out[i], out[i+1] = out[i+1], out[i]
	}

	return out
}

func TestAnalyze_LittleEndianTone_ScoresLittleEndianSmoother(t *testing.T) {
	tone := generateTone(toneFreqHz, 200*time.Millisecond, media.SampleRate)

	verdict := analyze(tone)

	assert.True(t, verdict.LittleEndian(), "a genuinely little-endian-encoded tone must score as little-endian")
	assert.Less(t, verdict.LittleEndianScore, verdict.BigEndianScore)
	assert.Equal(t, "le", verdict.EnvValue())
}

func TestAnalyze_BigEndianTone_ScoresBigEndianSmoother(t *testing.T) {
	// generateTone always produces LE bytes; byte-swapping a smooth waveform's LE
	// encoding produces exactly what that same waveform looks like on the wire if
	// it had actually been encoded big-endian.
	tone := swapBytes(generateTone(toneFreqHz, 200*time.Millisecond, media.SampleRate))

	verdict := analyze(tone)

	assert.False(t, verdict.LittleEndian(), "a genuinely big-endian-encoded tone must score as big-endian")
	assert.Less(t, verdict.BigEndianScore, verdict.LittleEndianScore)
	assert.Equal(t, "be", verdict.EnvValue())
}

func TestRequireCaptured_ZeroPackets_HardError(t *testing.T) {
	err := requireCaptured(nil, probeStats{rawDatagrams: 12, rtcpFiltered: 12})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no RTP captured")
	assert.Contains(t, err.Error(), "raw_datagrams=12")
	assert.Contains(t, err.Error(), "rtcp_filtered=12")
}

func TestRequireCaptured_ZeroRawDatagrams_HardErrorMentionsIt(t *testing.T) {
	// The other diagnosable failure mode: nothing reached the socket at all, not
	// even RTCP -- distinct from "datagrams arrived but were filtered out".
	err := requireCaptured(nil, probeStats{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "raw_datagrams=0")
}

func TestRequireCaptured_NonZeroCapture_NoError(t *testing.T) {
	err := requireCaptured([]byte{1, 2, 3, 4}, probeStats{captured: 1})

	assert.NoError(t, err)
}
