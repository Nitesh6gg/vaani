// Command endianness-check answers the question Phase 3 (STT) can't afford to
// assume: does Asterisk's externalMedia RTP L16 payload arrive little-endian or
// big-endian? RFC 3551 says L16 is big-endian, but that's the wire format for a
// generic RTP audio profile, not a guarantee about what a given Asterisk build
// actually emits on the externalMedia UDP transport -- so this is checked against
// a live server rather than assumed.
//
// It creates two externalMedia channels -- A in slin16 (where we send the test
// tone and capture the result) and B in ulaw (a different codec, forcing Asterisk
// to genuinely transcode when it bridges the two, exactly as it does for a real
// SIP call's audio) -- bridges them, sends a 2s 440Hz tone as little-endian PCM16
// into A, blindly relays whatever bytes arrive on B straight back out on B
// (no need to understand ulaw: Asterisk does the actual decode/re-encode on both
// sides of the bridge), and reports which byte order makes A's captured samples
// look like a smooth sine wave rather than scrambled noise -- byte-swapping a
// smooth waveform doesn't produce another smooth waveform, so the two
// interpretations are not ambiguous in practice. The result is appended to
// docs/AUDIO_PIPELINE.md.
//
// An earlier version of this tool used a dialplan Echo() leg instead of channel
// B, bridged directly with channel A. That doesn't work: ARI's bridge API only
// accepts channels under Stasis application control, and a channel running
// Echo() in the dialplan (not App: cfg.AriApp) is by definition not -- addChannel
// fails with 400 Bad Request. Being in Stasis and running a dialplan app are
// mutually exclusive for the same channel, so there's no way to patch that
// version; the two-externalMedia-channel design sidesteps the conflict entirely.
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

	portA, err := ports.Alloc() // slin16: tone source / capture
	if err != nil {
		return fmt.Errorf("allocate media port for channel A: %w", err)
	}
	defer ports.Free(portA)

	portB, err := ports.Alloc() // ulaw: forces real transcoding, blindly relayed
	if err != nil {
		return fmt.Errorf("allocate media port for channel B: %w", err)
	}
	defer ports.Free(portB)

	sub := cl.Bus().Subscribe(nil, "StasisStart")
	defer sub.Cancel()

	idA := rid.New(rid.Channel)
	idB := rid.New(rid.Channel)

	slog.Info("creating externalMedia channel A (slin16, tone source)", "channel_id", idA, "port", portA)

	if _, err := cl.Channel().ExternalMedia(ari.NewKey(ari.ChannelKey, idA), ari.ExternalMediaOptions{
		ChannelID:     idA,
		App:           cfg.AriApp,
		ExternalHost:  fmt.Sprintf("%s:%d", cfg.MediaIP, portA),
		Encapsulation: "rtp",
		Transport:     "udp",
		Format:        "slin16",
	}); err != nil {
		return fmt.Errorf("create externalMedia channel A: %w", err)
	}
	defer func() { _ = cl.Channel().Hangup(ari.NewKey(ari.ChannelKey, idA), "") }()

	slog.Info("creating externalMedia channel B (ulaw, forces transcoding)", "channel_id", idB, "port", portB)

	if _, err := cl.Channel().ExternalMedia(ari.NewKey(ari.ChannelKey, idB), ari.ExternalMediaOptions{
		ChannelID:     idB,
		App:           cfg.AriApp,
		ExternalHost:  fmt.Sprintf("%s:%d", cfg.MediaIP, portB),
		Encapsulation: "rtp",
		Transport:     "udp",
		Format:        "ulaw",
	}); err != nil {
		return fmt.Errorf("create externalMedia channel B: %w", err)
	}
	defer func() { _ = cl.Channel().Hangup(ari.NewKey(ari.ChannelKey, idB), "") }()

	if err := waitForStasisStarts(ctx, sub, []string{idA, idB}, 10*time.Second); err != nil {
		return fmt.Errorf("waiting for externalMedia channels: %w", err)
	}

	bridgeKey := ari.NewKey(ari.BridgeKey, rid.New(rid.Bridge))

	bh, err := cl.Bridge().Create(bridgeKey, "mixing", bridgeKey.ID)
	if err != nil {
		return fmt.Errorf("create bridge: %w", err)
	}
	defer func() { _ = bh.Delete() }()

	if err := bh.AddChannel(idA); err != nil {
		return fmt.Errorf("add channel A to bridge: %w", err)
	}

	if err := bh.AddChannel(idB); err != nil {
		return fmt.Errorf("add channel B to bridge: %w", err)
	}

	slog.Info("bridged, running tone probe", "port_a", portA, "port_b", portB, "duration", toneDuration)

	relayStop := make(chan struct{})
	relayDone := make(chan struct{})

	go func() {
		defer close(relayDone)

		if err := relayRaw(portB, relayStop); err != nil {
			slog.Error("channel B relay error", "error", err)
		}
	}()

	captured, stats, err := probe(portA)

	close(relayStop)
	<-relayDone

	if err != nil {
		return fmt.Errorf("probe: %w", err)
	}

	slog.Info("probe capture stats",
		"raw_datagrams", stats.rawDatagrams,
		"rtcp_filtered", stats.rtcpFiltered,
		"parse_failed", stats.parseFailed,
		"rtp_packets_captured", stats.captured,
		"bytes_captured", len(captured))

	if err := requireCaptured(captured, stats); err != nil {
		return err
	}

	verdict := analyze(captured)
	slog.Info("endianness verdict", "result", verdict.String())

	fmt.Println(verdict.String())
	fmt.Printf("\nSet this in the service's environment (.env or real env):\n\nAUDIO_L16_ENDIANNESS=%s\n\n", verdict.EnvValue())

	return appendToDocs(verdict)
}

// waitForStasisStarts waits for a StasisStart event for every channel in ids,
// in any order, from a single shared subscription. This must handle all of them
// in one loop rather than one call per ID: draining the subscription once per ID
// would discard any event for a not-yet-awaited ID encountered along the way,
// since a plain Subscription has no way to put an event back.
func waitForStasisStarts(ctx context.Context, sub ari.Subscription, ids []string, timeout time.Duration) error {
	remaining := make(map[string]bool, len(ids))
	for _, id := range ids {
		remaining[id] = true
	}

	deadline := time.After(timeout)

	for len(remaining) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			pending := make([]string, 0, len(remaining))
			for id := range remaining {
				pending = append(pending, id)
			}

			return fmt.Errorf("timed out waiting for channels to enter Stasis: %v", pending)
		case evt, ok := <-sub.Events():
			if !ok {
				return fmt.Errorf("event bus closed while waiting for channels to enter Stasis")
			}

			if start, ok := evt.(*ari.StasisStart); ok {
				delete(remaining, start.Channel.ID)
			}
		}
	}

	return nil
}

// relayRaw blindly bounces every UDP datagram received on port straight back to
// whichever address it arrived from, unmodified. Used for channel B: since
// Asterisk does the ulaw<->slin16 transcoding on both sides of the bridge, this
// relay never needs to understand the bytes it's reflecting.
func relayRaw(port int, stop <-chan struct{}) error {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	buf := make([]byte, 1500)

	for {
		select {
		case <-stop:
			return nil
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))

		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue // read timeout; loop back around to check stop
		}

		_, _ = conn.WriteToUDP(buf[:n], addr)
	}
}

// probeStats counts what happened at each stage of probe's reader loop, so a
// zero-capture run can be diagnosed instead of just failing mysteriously: how
// many raw UDP datagrams arrived at all, how many of those were RTCP (no audio
// content), how many failed RTP parsing, and how many were captured as real
// audio payload.
type probeStats struct {
	rawDatagrams int
	rtcpFiltered int
	parseFailed  int
	captured     int
}

// probe sends a 2s 440Hz little-endian PCM16 tone over RTP on port, one 20ms frame
// at a time, and returns every inbound RTP payload received during that window
// (plus a short grace period) concatenated in arrival order.
func probe(port int) ([]byte, probeStats, error) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
	if err != nil {
		return nil, probeStats{}, err
	}
	defer func() { _ = conn.Close() }()

	ep := media.NewEndpoint(conn)
	sender := media.NewSender()

	captured := &bytes.Buffer{}
	captureDone := make(chan struct{})

	var stats probeStats

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

			stats.rawDatagrams++

			// Lock as soon as ANY datagram arrives on this leg's muxed RTP/RTCP
			// socket -- not gated on successfully parsing real RTP audio. This
			// fixes a real deadlock: Asterisk won't send audio back on this leg
			// until it can already reach our tone-sender's Endpoint.WriteTo, which
			// itself refuses to send until LockRemote has fired. Gating the lock on
			// "valid RTP audio arrived" makes that impossible to ever satisfy if
			// Asterisk starts this leg with RTCP-only traffic (or nothing) before
			// it has real content to mix -- our sender can never produce the "valid
			// RTP audio" the old gate was waiting for without the lock it's itself
			// blocking. See docs/AUDIO_PIPELINE.md.
			if ep.LockRemote(addr) {
				slog.Info("probe remote locked", "addr", addr)
			}

			if media.IsRTCP(buf[:n]) {
				stats.rtcpFiltered++
				continue
			}

			pkt, err := media.ParseRTP(buf[:n])
			if err != nil {
				stats.parseFailed++
				continue
			}

			stats.captured++
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

	return captured.Bytes(), stats, nil
}

// requireCaptured fails loudly instead of letting a zero-sample capture flow
// into analyze(), which would otherwise score two empty slices as equally
// "smooth" (0.0 <= 0.0) and print a confident-looking but meaningless verdict.
// stats' per-stage counts are included so a real zero-capture run points
// straight at where the flow stopped (e.g. rawDatagrams==0 means nothing ever
// reached this socket at all; rawDatagrams>0 but captured==0 means datagrams
// arrived but were all RTCP or failed RTP parsing).
func requireCaptured(captured []byte, stats probeStats) error {
	if len(captured) == 0 {
		return fmt.Errorf(
			"no RTP captured — probe did not flow (raw_datagrams=%d rtcp_filtered=%d parse_failed=%d)",
			stats.rawDatagrams, stats.rtcpFiltered, stats.parseFailed)
	}

	return nil
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

// EnvValue returns the AUDIO_L16_ENDIANNESS value ("le" or "be") this verdict
// implies, ready to paste into the service's environment/.env.
func (v Verdict) EnvValue() string {
	if v.LittleEndian() {
		return "le"
	}

	return "be"
}

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
