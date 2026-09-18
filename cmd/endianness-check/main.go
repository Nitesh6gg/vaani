// Command endianness-check answers the question Phase 3 (STT) can't afford to
// assume: does Asterisk's externalMedia RTP L16 payload arrive little-endian or
// big-endian? RFC 3551 says L16 is big-endian, but that's the wire format for a
// generic RTP audio profile, not a guarantee about what a given Asterisk build
// actually emits on the externalMedia UDP transport -- so this is checked against
// a live server rather than assumed.
//
// It originates a dialplan Echo() leg (deploy/asterisk/extensions.conf, context
// vaani-tools, extension 9001), bridges it with a second externalMedia channel,
// sends a 2s 440Hz tone as little-endian PCM16, captures what echoes back, and
// reports which byte order makes the captured samples look like a smooth sine wave
// rather than scrambled noise -- byte-swapping a smooth waveform doesn't produce
// another smooth waveform, so the two interpretations are not ambiguous in
// practice. The result is appended to docs/AUDIO_PIPELINE.md.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/rid"

	ariclient "github.com/nitesh/vaani/internal/ari"
	"github.com/nitesh/vaani/internal/config"
	"github.com/nitesh/vaani/internal/media"
)

const (
	echoContext  = "vaani-tools"
	echoExten    = "9001"
	toneFreqHz   = 440.0
	toneDuration = 2 * time.Second
	captureGrace = 500 * time.Millisecond // linger after the tone stops, for tail packets
)

func main() {
	if err := run(); err != nil {
		slog.Error("endianness-check failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// A dedicated Stasis app so this tool can run independently of cmd/server.
	if v := os.Getenv("ENDIAN_ARI_APP"); v != "" {
		cfg.AriApp = v
	} else {
		cfg.AriApp = "vaani-endiancheck"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cl, err := ariclient.Connect(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer cl.Close()

	ports := media.NewPortAllocator(cfg.MediaPortBase, cfg.MediaPortCount)

	port, err := ports.Alloc()
	if err != nil {
		return fmt.Errorf("allocate media port: %w", err)
	}
	defer ports.Free(port)

	sub := cl.Bus().Subscribe(nil, "StasisStart")
	defer sub.Cancel()

	echoID := rid.New(rid.Channel)

	slog.Info("originating echo leg", "channel_id", echoID)

	if _, err := cl.Channel().Originate(nil, ari.OriginateRequest{
		Endpoint:  fmt.Sprintf("Local/%s@%s", echoExten, echoContext),
		Context:   echoContext,
		Extension: echoExten,
		Priority:  1,
		ChannelID: echoID,
		Timeout:   10,
	}); err != nil {
		return fmt.Errorf("originate echo leg: %w", err)
	}
	defer func() { _ = cl.Channel().Hangup(ari.NewKey(ari.ChannelKey, echoID), "") }()

	externalID := rid.New(rid.Channel)

	slog.Info("creating externalMedia channel", "channel_id", externalID, "port", port)

	if _, err := cl.Channel().ExternalMedia(ari.NewKey(ari.ChannelKey, externalID), ari.ExternalMediaOptions{
		ChannelID:     externalID,
		App:           cfg.AriApp,
		ExternalHost:  fmt.Sprintf("%s:%d", cfg.MediaIP, port),
		Encapsulation: "rtp",
		Transport:     "udp",
		Format:        "slin16",
	}); err != nil {
		return fmt.Errorf("create externalMedia channel: %w", err)
	}
	defer func() { _ = cl.Channel().Hangup(ari.NewKey(ari.ChannelKey, externalID), "") }()

	if err := waitForStasisStart(ctx, sub, externalID, 10*time.Second); err != nil {
		return fmt.Errorf("waiting for externalMedia channel: %w", err)
	}

	bridgeKey := ari.NewKey(ari.BridgeKey, rid.New(rid.Bridge))

	bh, err := cl.Bridge().Create(bridgeKey, "mixing", bridgeKey.ID)
	if err != nil {
		return fmt.Errorf("create bridge: %w", err)
	}
	defer func() { _ = bh.Delete() }()

	// Give Echo() a moment to finish answering before bridging.
	time.Sleep(300 * time.Millisecond)

	if err := bh.AddChannel(echoID); err != nil {
		return fmt.Errorf("add echo channel to bridge: %w", err)
	}

	if err := bh.AddChannel(externalID); err != nil {
		return fmt.Errorf("add externalMedia channel to bridge: %w", err)
	}

	slog.Info("bridged, running tone probe", "port", port, "duration", toneDuration)

	captured, err := probe(port)
	if err != nil {
		return fmt.Errorf("probe: %w", err)
	}

	verdict := analyze(captured)
	slog.Info("endianness verdict", "result", verdict.String())

	fmt.Println(verdict.String())

	return appendToDocs(verdict)
}

func waitForStasisStart(ctx context.Context, sub ari.Subscription, channelID string, timeout time.Duration) error {
	deadline := time.After(timeout)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("timed out waiting for channel %s to enter Stasis", channelID)
		case evt, ok := <-sub.Events():
			if !ok {
				return fmt.Errorf("event bus closed while waiting for channel %s", channelID)
			}

			if start, ok := evt.(*ari.StasisStart); ok && start.Channel.ID == channelID {
				return nil
			}
		}
	}
}

// probe sends a 2s 440Hz little-endian PCM16 tone over RTP on port, one 20ms frame
// at a time, and returns every inbound RTP payload received during that window
// (plus a short grace period) concatenated in arrival order.
func probe(port int) ([]byte, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	ep := media.NewEndpoint(conn)
	sender := media.NewSender()

	captured := &bytes.Buffer{}
	captureDone := make(chan struct{})

	readCtx, cancelRead := context.WithCancel(context.Background())
	defer cancelRead()

	go func() {
		defer close(captureDone)

		buf := make([]byte, 1500)

		for {
			_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))

			n, addr, err := ep.ReadFrom(buf)
			if err != nil {
				if readCtx.Err() != nil {
					return
				}

				continue // read timeout; keep polling until readCtx is cancelled
			}

			if media.IsRTCP(buf[:n]) {
				continue
			}

			pkt, err := media.ParseRTP(buf[:n])
			if err != nil {
				continue
			}

			ep.LockRemote(addr)
			captured.Write(pkt.Payload)
		}
	}()

	tone := generateTone(toneFreqHz, toneDuration, media.SampleRate)

	pacer := media.NewPacer(media.FrameInterval)

	frameCount := len(tone) / media.FrameSize
	sent := 0

	done := make(chan struct{})

	go func() {
		pacer.Run(func(_ time.Time) {
			if sent >= frameCount {
				pacer.Stop()
				return
			}

			frame := tone[sent*media.FrameSize : (sent+1)*media.FrameSize]
			sent++

			pkt := sender.Build(frame)

			out, err := pkt.Marshal()
			if err != nil {
				return
			}

			_, _ = ep.WriteTo(out)
		})
		close(done)
	}()

	<-done
	time.Sleep(captureGrace)
	cancelRead()
	<-captureDone

	return captured.Bytes(), nil
}

// generateTone renders a pure sine wave as little-endian PCM16 at sampleRate,
// zero-padded up to a whole number of media.FrameSize frames.
func generateTone(freqHz float64, dur time.Duration, sampleRate int) []byte {
	n := int(dur.Seconds() * float64(sampleRate))

	buf := make([]byte, 0, n*2)
	for i := 0; i < n; i++ {
		t := float64(i) / float64(sampleRate)
		sample := int16(math.Sin(2*math.Pi*freqHz*t) * 0.8 * math.MaxInt16)

		var b [2]byte
		binary.LittleEndian.PutUint16(b[:], uint16(sample))
		buf = append(buf, b[:]...)
	}

	if rem := len(buf) % media.FrameSize; rem != 0 {
		buf = append(buf, make([]byte, media.FrameSize-rem)...)
	}

	return buf
}

// Verdict is the outcome of comparing little-endian vs big-endian interpretations
// of the captured samples.
type Verdict struct {
	LittleEndianScore float64
	BigEndianScore    float64
	SampleCount       int
}

// LittleEndian reports whether the little-endian interpretation is the smoother
// (and therefore correct) one.
func (v Verdict) LittleEndian() bool { return v.LittleEndianScore <= v.BigEndianScore }

func (v Verdict) String() string {
	order := "big-endian (s16be)"
	if v.LittleEndian() {
		order = "little-endian (s16le)"
	}

	return fmt.Sprintf(
		"externalMedia RTP L16 payload is %s (n=%d samples, le_smoothness=%.1f, be_smoothness=%.1f -- lower is smoother)",
		order, v.SampleCount, v.LittleEndianScore, v.BigEndianScore,
	)
}

// analyze decodes raw as both little- and big-endian PCM16 and scores each by mean
// absolute sample-to-sample delta. A real audio waveform has small deltas; swapping
// the bytes of a smooth waveform scrambles it into large, near-random deltas. The
// lower-scoring interpretation is the correct one.
func analyze(raw []byte) Verdict {
	n := len(raw) / 2

	le := make([]int16, n)
	be := make([]int16, n)

	for i := 0; i < n; i++ {
		le[i] = int16(binary.LittleEndian.Uint16(raw[i*2:]))
		be[i] = int16(binary.BigEndian.Uint16(raw[i*2:]))
	}

	return Verdict{
		LittleEndianScore: meanAbsDelta(le),
		BigEndianScore:    meanAbsDelta(be),
		SampleCount:       n,
	}
}

func meanAbsDelta(samples []int16) float64 {
	if len(samples) < 2 {
		return 0
	}

	var sum float64

	for i := 1; i < len(samples); i++ {
		d := int(samples[i]) - int(samples[i-1])
		if d < 0 {
			d = -d
		}

		sum += float64(d)
	}

	return sum / float64(len(samples)-1)
}

// appendToDocs writes the verdict into docs/AUDIO_PIPELINE.md so Phase 3 doesn't
// have to re-derive it or take it on faith from a log line.
func appendToDocs(v Verdict) error {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("could not locate docs/AUDIO_PIPELINE.md relative to this binary's source")
	}

	docPath := filepath.Join(filepath.Dir(thisFile), "..", "..", "docs", "AUDIO_PIPELINE.md")

	entry := fmt.Sprintf(
		"\n## Endianness Verification Result (recorded %s)\n\n%s\n",
		time.Now().UTC().Format(time.RFC3339), v.String(),
	)

	f, err := os.OpenFile(docPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("append to %s: %w", docPath, err)
	}

	_, writeErr := f.WriteString(entry)
	closeErr := f.Close()

	if writeErr != nil {
		return fmt.Errorf("append to %s: %w", docPath, writeErr)
	}

	return closeErr
}
