package media

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAudioSocketFrame_RoundTrip_Audio(t *testing.T) {
	payload := make([]byte, FrameSize)
	for i := range payload {
		payload[i] = byte(i)
	}

	buf := &bytes.Buffer{}
	require.NoError(t, WriteAudioSocketFrame(buf, AudioSocketKindSlin16, payload))

	got, err := ReadAudioSocketFrame(buf)
	require.NoError(t, err)

	assert.Equal(t, byte(AudioSocketKindSlin16), got.Kind)
	assert.Equal(t, payload, got.Payload)

	PutFrame(&got.Payload)
}

func TestAudioSocketFrame_RoundTrip_ControlFrame(t *testing.T) {
	uuid := []byte("0123456789abcdef") // 16 bytes, not FrameSize -- must not come from the pool

	buf := &bytes.Buffer{}
	require.NoError(t, WriteAudioSocketFrame(buf, AudioSocketKindUUID, uuid))

	got, err := ReadAudioSocketFrame(buf)
	require.NoError(t, err)

	assert.Equal(t, byte(AudioSocketKindUUID), got.Kind)
	assert.Equal(t, uuid, got.Payload)
}

func TestAudioSocketFrame_HangupHasNoPayload(t *testing.T) {
	buf := &bytes.Buffer{}
	require.NoError(t, WriteAudioSocketFrame(buf, AudioSocketKindHangup, nil))

	got, err := ReadAudioSocketFrame(buf)
	require.NoError(t, err)

	assert.Equal(t, byte(AudioSocketKindHangup), got.Kind)
	assert.Empty(t, got.Payload)
}

func TestAudioSocketFrame_WireFormatMatchesSpec(t *testing.T) {
	payload := make([]byte, FrameSize)

	buf := &bytes.Buffer{}
	require.NoError(t, WriteAudioSocketFrame(buf, AudioSocketKindSlin16, payload))

	raw := buf.Bytes()
	require.Len(t, raw, audioSocketHeaderSize+FrameSize)

	assert.Equal(t, byte(0x12), raw[0], "kind byte for slin16 must be 0x12")
	assert.Equal(t, byte(0x02), raw[1], "length high byte: 640 = 0x0280")
	assert.Equal(t, byte(0x80), raw[2], "length low byte: 640 = 0x0280")
}

func TestReadAudioSocketFrame_TruncatedHeader(t *testing.T) {
	buf := bytes.NewReader([]byte{0x12, 0x02}) // header needs 3 bytes, only 2 given

	_, err := ReadAudioSocketFrame(buf)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestReadAudioSocketFrame_TruncatedPayload(t *testing.T) {
	// Header claims 640 bytes of payload but only 10 are actually present.
	buf := bytes.NewReader(append([]byte{0x12, 0x02, 0x80}, make([]byte, 10)...))

	_, err := ReadAudioSocketFrame(buf)
	assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
}

func TestWriteAudioSocketFrame_RejectsOversizedPayload(t *testing.T) {
	buf := &bytes.Buffer{}
	err := WriteAudioSocketFrame(buf, AudioSocketKindSlin16, make([]byte, audioSocketMaxPayload+1))
	assert.ErrorIs(t, err, ErrAudioSocketFrameTooLarge)
}

// TestAudioSocketFrame_MultipleFramesOnOneStream proves frames can be read back
// to back from a single stream (the real TCP usage pattern), since AudioSocket
// carries a continuous sequence of frames, not one per connection.
func TestAudioSocketFrame_MultipleFramesOnOneStream(t *testing.T) {
	buf := &bytes.Buffer{}

	for i := 0; i < 5; i++ {
		payload := make([]byte, FrameSize)
		payload[0] = byte(i)
		require.NoError(t, WriteAudioSocketFrame(buf, AudioSocketKindSlin16, payload))
	}

	for i := 0; i < 5; i++ {
		got, err := ReadAudioSocketFrame(buf)
		require.NoError(t, err)
		assert.Equal(t, byte(i), got.Payload[0])
		PutFrame(&got.Payload)
	}

	_, err := ReadAudioSocketFrame(buf)
	assert.ErrorIs(t, err, io.EOF, "stream should be exhausted after reading all 5 frames")
}
