package ari

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CyCoreSystems/ari/v5"
	"github.com/CyCoreSystems/ari/v5/client/arimocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/config"
	"github.com/nitesh/vaani/internal/dograh"
	"github.com/nitesh/vaani/internal/media"
	"github.com/nitesh/vaani/internal/session"
)

type outboundRig struct {
	m        *Manager
	channels *arimocks.Channel
	ports    *media.PortAllocator

	mu       sync.Mutex
	query    url.Values // of the last originate request
	user     string
	status   int    // what the fake Asterisk answers originate with
	reply    string // and its body
	hungUp   []string
	outcomes chan Outcome
}

func newOutboundRig(t *testing.T) *outboundRig {
	t.Helper()

	r := &outboundRig{channels: &arimocks.Channel{}, outcomes: make(chan Outcome, 4)}

	r.status = http.StatusOK

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		defer r.mu.Unlock()

		r.query = req.URL.Query()
		r.user, _, _ = req.BasicAuth()

		w.WriteHeader(r.status)
		_, _ = w.Write([]byte(r.reply))
	}))
	t.Cleanup(srv.Close)
	r.channels.On("Hangup", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		r.mu.Lock()
		r.hungUp = append(r.hungUp, args.Get(0).(*ari.Key).ID)
		r.mu.Unlock()
	}).Return(nil)

	cl := &arimocks.Client{}
	cl.On("Channel").Return(r.channels)

	r.ports = media.NewPortAllocator(21000, 4)
	r.m = NewManager(cl, config.Config{AriApp: "vaani", MediaIP: "127.0.0.1", AriURL: srv.URL, AriUser: "vaani", AriPass: "pw"}, r.ports, nil, nil)
	t.Cleanup(r.m.teardownAll) // closes the media sockets of calls a test left staged

	return r
}

func (r *outboundRig) dial(t *testing.T, ring time.Duration) string {
	t.Helper()

	id, err := r.m.Dial(context.Background(), DialRequest{
		Endpoint: "PJSIP/9876543210@trunk", CallerID: "7317185732", Called: "9876543210",
		RingTimeout: ring, WorkflowID: 19, RunID: 931, Variables: map[string]any{"name": "Asha"},
		Done: func(o Outcome) { r.outcomes <- o },
	})
	require.NoError(t, err)

	return id
}

func (r *outboundRig) outcome(t *testing.T) Outcome {
	t.Helper()

	select {
	case o := <-r.outcomes:
		return o
	case <-time.After(2 * time.Second):
		t.Fatal("no outcome")

		return Outcome{}
	}
}

func (r *outboundRig) noMoreOutcomes(t *testing.T) {
	t.Helper()

	select {
	case o := <-r.outcomes:
		t.Fatalf("unexpected second outcome %+v", o)
	case <-time.After(50 * time.Millisecond):
	}
}

func destroyed(id string, cause int) *ari.ChannelDestroyed {
	return &ari.ChannelDestroyed{Cause: cause, CauseTxt: "x", Channel: ari.ChannelData{ID: id}}
}

func TestDial_OriginatesIntoStasisWithTheRun(t *testing.T) {
	r := newOutboundRig(t)
	id := r.dial(t, 30*time.Second)

	r.mu.Lock()
	defer r.mu.Unlock()

	assert.Equal(t, url.Values{
		"endpoint": {"PJSIP/9876543210@trunk"}, "app": {"vaani"}, "appArgs": {"outbound,931"},
		"timeout": {"30"}, "channelId": {id}, "callerId": {"7317185732"},
	}, r.query, "as query parameters, the form Asterisk 20 accepts")
	assert.Equal(t, "vaani", r.user, "with the ARI credentials")
}

func TestDial_FailureCarriesAsterisksExplanation(t *testing.T) {
	r := newOutboundRig(t)
	r.status, r.reply = http.StatusBadRequest, `{"message":"Invalid parameters for originating a channel"}`

	id, err := r.m.Dial(context.Background(), DialRequest{Endpoint: "SIP/1@x", RingTimeout: time.Second, Done: func(Outcome) { t.Error("Done must not be called") }})

	require.Error(t, err)
	assert.Empty(t, id)
	assert.ErrorContains(t, err, "400 Bad Request")
	assert.ErrorContains(t, err, "Invalid parameters for originating a channel")
	assert.Empty(t, r.m.outbound, "nothing is waiting for a call that was never placed")
}

func TestDial_NoCallerIDIsNotSent(t *testing.T) {
	r := newOutboundRig(t)

	_, err := r.m.Dial(context.Background(), DialRequest{Endpoint: "SIP/1@x", RingTimeout: time.Second})
	require.NoError(t, err)

	r.mu.Lock()
	defer r.mu.Unlock()

	assert.NotContains(t, r.query, "callerId")
}

func TestDial_UnansweredStatusFromHangupCause(t *testing.T) {
	for cause, want := range map[int]string{
		17: dograh.StatusBusy, 19: dograh.StatusNoAnswer, 21: dograh.StatusRejected,
		16: dograh.StatusFailed, 34: dograh.StatusFailed,
	} {
		r := newOutboundRig(t)
		id := r.dial(t, 30*time.Second)

		r.m.onOutboundDestroyed(destroyed(id, cause))

		assert.Equal(t, Outcome{Status: want, Cause: cause}, r.outcome(t), "cause %d", cause)

		r.m.onOutboundDestroyed(destroyed(id, cause))
		r.noMoreOutcomes(t)
		assert.Empty(t, r.m.outbound, "forgotten once over")
	}
}

func TestDial_DestroyedEventsOfOtherChannelsAreIgnored(t *testing.T) {
	r := newOutboundRig(t)
	r.dial(t, 30*time.Second)

	r.m.onOutboundDestroyed(destroyed("someone-elses-channel", 17))
	r.noMoreOutcomes(t)
	assert.Len(t, r.m.outbound, 1)
}

func TestDial_WatchdogEndsACallNobodyReportedOn(t *testing.T) {
	old := outboundAnswerSlack
	outboundAnswerSlack = 20 * time.Millisecond

	t.Cleanup(func() { outboundAnswerSlack = old })

	r := newOutboundRig(t)
	id := r.dial(t, 0)

	assert.Equal(t, Outcome{Status: dograh.StatusNoAnswer}, r.outcome(t))

	r.mu.Lock()
	assert.Contains(t, r.hungUp, id, "the ringing channel is hung up")
	r.mu.Unlock()
}

func TestDial_AnsweredCallIsNotAnsweredAgainAndEndsConnected(t *testing.T) {
	r := newOutboundRig(t)
	id := r.dial(t, 30*time.Second)

	// No expectation for Answer: the mock panics if the channel is answered.
	r.channels.On("Get", mock.Anything).Return(ari.NewChannelHandle(ari.NewKey(ari.ChannelKey, id), r.channels, nil))
	r.channels.On("ExternalMedia", mock.Anything, mock.Anything).Return(nil, nil)

	r.m.onOutboundAnswered(context.Background(), &ari.StasisStart{Args: []string{"outbound", "931"}, Channel: ari.ChannelData{ID: id}})

	r.m.mu.Lock()
	c := r.m.pending[id]
	r.m.mu.Unlock()
	require.NotNil(t, c, "the call is staged like an inbound one")

	assert.Equal(t, "outbound", c.Info.Direction)
	assert.Equal(t, "7317185732", c.Info.CallerNumber, "the caller ID we showed")
	assert.Equal(t, "9876543210", c.Info.CalledNumber)

	// The ringing watchdog must not fire on an answered call.
	r.m.outboundTimedOut(id)
	r.noMoreOutcomes(t)

	// The agent started, then the call ended.
	r.m.mediaHandler(c)
	r.m.teardown(c)

	assert.Equal(t, Outcome{Connected: true}, r.outcome(t))
	r.noMoreOutcomes(t)
	assert.Empty(t, r.m.outbound)
	assert.Zero(t, r.ports.InUse())
}

func TestDial_AnsweredButNeverReachedTheAgentIsNotConnected(t *testing.T) {
	r := newOutboundRig(t)
	id := r.dial(t, 30*time.Second)

	r.channels.On("Get", mock.Anything).Return(ari.NewChannelHandle(ari.NewKey(ari.ChannelKey, id), r.channels, nil))
	r.channels.On("ExternalMedia", mock.Anything, mock.Anything).Return(nil, nil)

	r.m.onOutboundAnswered(context.Background(), &ari.StasisStart{Args: []string{"outbound", "931"}, Channel: ari.ChannelData{ID: id}})

	r.m.mu.Lock()
	c := r.m.pending[id]
	r.m.mu.Unlock()
	require.NotNil(t, c)

	r.m.abort(c) // the bridge could not be built

	assert.Equal(t, Outcome{Status: dograh.StatusFailed}, r.outcome(t))
	assert.Zero(t, r.ports.InUse())
}

func TestDial_AnswerWithNobodyWaitingIsHungUp(t *testing.T) {
	r := newOutboundRig(t)

	r.m.onOutboundAnswered(context.Background(), &ari.StasisStart{Args: []string{"outbound", "1"}, Channel: ari.ChannelData{ID: "stale"}})

	r.mu.Lock()
	assert.Equal(t, []string{"stale"}, r.hungUp)
	r.mu.Unlock()
}

func TestDial_ShutdownHangsUpWhatIsRinging(t *testing.T) {
	r := newOutboundRig(t)
	id := r.dial(t, 30*time.Second)

	r.m.teardownAll()

	assert.Equal(t, Outcome{Status: dograh.StatusFailed}, r.outcome(t))

	r.mu.Lock()
	assert.Contains(t, r.hungUp, id)
	r.mu.Unlock()
}

// Over the CPU limit a new caller is hung up as congestion before it is
// answered, and nothing is allocated for it.
func TestStartCall_RefusedWhileOverloaded(t *testing.T) {
	r := newOutboundRig(t)

	var over atomic.Bool

	r.m.SetOverloaded(over.Load)
	over.Store(true)

	r.m.onStasisStart(context.Background(), &ari.StasisStart{Channel: ari.ChannelData{ID: "caller-1"}})

	r.channels.AssertCalled(t, "Hangup", mock.Anything, "congestion")
	r.channels.AssertNotCalled(t, "Get", mock.Anything)
	assert.Zero(t, r.ports.InUse(), "no media port was taken")
	assert.Empty(t, r.m.pending)
	assert.Equal(t, []string{"caller-1"}, r.hungUp)
}

// A call Vaani dialed is already answered; it is not refused here (the dialer
// stops placing calls while overloaded).
func TestStartCall_OutboundAnswerIsNotRefused(t *testing.T) {
	r := newOutboundRig(t)
	r.m.SetOverloaded(func() bool { return true })

	id := r.dial(t, 30*time.Second)
	r.channels.On("Get", mock.Anything).Return(ari.NewChannelHandle(ari.NewKey(ari.ChannelKey, id), r.channels, nil))
	r.channels.On("ExternalMedia", mock.Anything, mock.Anything).Return(nil, nil)

	r.m.onOutboundAnswered(context.Background(), &ari.StasisStart{Args: []string{"outbound", "931"}, Channel: ari.ChannelData{ID: id}})

	r.m.mu.Lock()
	_, staged := r.m.pending[id]
	r.m.mu.Unlock()

	assert.True(t, staged, "staged like any answered call")
	r.channels.AssertNotCalled(t, "Hangup", mock.Anything, "congestion")
}

// fakeSub is an ARI subscription whose buffer is as small as the library's.
type fakeSub struct{ c chan ari.Event }

func (f *fakeSub) Events() <-chan ari.Event { return f.c }
func (f *fakeSub) Cancel()                  {}

// The pump moves events out of the library's 100-event buffer at once, however
// slowly they are then handled; the library drops what doesn't fit.
func TestPump_NeverWaitsOnTheHandlers(t *testing.T) {
	r := newOutboundRig(t)
	sub := &fakeSub{c: make(chan ari.Event, 100)}
	events := make(chan ari.Event, eventQueueSize) // nobody reads it: the handlers are stuck

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go r.m.pump(ctx, sub, events)

	sent := make(chan struct{})

	go func() {
		defer close(sent)

		for i := 0; i < 3000; i++ {
			sub.c <- &ari.StasisEnd{} // a blocking send: it only completes if the pump keeps taking events
		}
	}()

	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("the pump stalled: the library's buffer would have dropped events")
	}

	require.Eventually(t, func() bool { return len(events) >= 2900 }, 2*time.Second, time.Millisecond)
}

func TestCallKey_AnExternalMediaChannelIsKeyedByItsCall(t *testing.T) {
	r := newOutboundRig(t)
	c := &call{Call: session.New("caller-1", "ext-1", 21000, session.Info{}, context.Background())}

	r.m.mu.Lock()
	r.m.pending["caller-1"], r.m.pending["ext-1"] = c, c
	r.m.mu.Unlock()

	assert.Equal(t, "caller-1", r.m.callKey("ext-1"))
	assert.Equal(t, "caller-1", r.m.callKey("caller-1"))
	assert.Equal(t, "other", r.m.callKey("other"))
	assert.Equal(t, 1, r.m.SettingUp(), "one staged call, in pending under both its IDs")
}

// A staged call whose media channel never enters Stasis is hung up, its port
// freed, instead of sitting on Asterisk forever.
func TestStartCall_SetupTimesOutWhenTheMediaChannelNeverArrives(t *testing.T) {
	r := newOutboundRig(t)

	old := setupTimeout
	setupTimeout = 400 * time.Millisecond

	t.Cleanup(func() { setupTimeout = old })

	r.channels.On("Get", mock.Anything).Return(ari.NewChannelHandle(ari.NewKey(ari.ChannelKey, "caller-1"), r.channels, nil))
	r.channels.On("Answer", mock.Anything).Return(nil)
	r.channels.On("ExternalMedia", mock.Anything, mock.Anything).Return(nil, nil)

	r.m.onStasisStart(context.Background(), &ari.StasisStart{Channel: ari.ChannelData{ID: "caller-1"}})
	require.Equal(t, 1, r.m.SettingUp())
	require.Equal(t, 1, r.ports.InUse())

	hung := func() []string {
		r.mu.Lock()
		defer r.mu.Unlock()

		return append([]string(nil), r.hungUp...)
	}

	require.Eventually(t, func() bool { return len(hung()) == 2 }, 3*time.Second, 10*time.Millisecond)

	assert.Contains(t, hung(), "caller-1", "the caller is hung up, and the media channel too")
	assert.Zero(t, r.m.SettingUp())
	assert.Zero(t, r.ports.InUse(), "the port is free again")
}

// The timeout runs in the call's queue: while that call's events are being
// handled it waits, so it can't abort a call its StasisStart is bridging.
func TestStartCall_SetupTimeoutWaitsForTheCallsQueue(t *testing.T) {
	r := newOutboundRig(t)

	old := setupTimeout
	setupTimeout = 100 * time.Millisecond

	t.Cleanup(func() { setupTimeout = old })

	r.channels.On("Get", mock.Anything).Return(ari.NewChannelHandle(ari.NewKey(ari.ChannelKey, "caller-1"), r.channels, nil))
	r.channels.On("Answer", mock.Anything).Return(nil)
	r.channels.On("ExternalMedia", mock.Anything, mock.Anything).Return(nil, nil)

	r.m.onStasisStart(context.Background(), &ari.StasisStart{Channel: ari.ChannelData{ID: "caller-1"}})
	require.Equal(t, 1, r.m.SettingUp())

	release := make(chan struct{})
	r.m.exec.do("caller-1", func() { <-release }) // the call's queue is busy

	hung := func() int {
		r.mu.Lock()
		defer r.mu.Unlock()

		return len(r.hungUp)
	}

	time.Sleep(300 * time.Millisecond) // past the timeout
	assert.Zero(t, hung(), "the timeout waits behind the call's running handler")
	assert.Equal(t, 1, r.m.SettingUp())

	close(release)
	require.Eventually(t, func() bool { return hung() == 2 }, 3*time.Second, 10*time.Millisecond)
	assert.Zero(t, r.m.SettingUp())
}

// The media channel arriving in time stops the timer.
func TestStartCall_MediaChannelInTimeKeepsTheCall(t *testing.T) {
	r := newOutboundRig(t)

	old := setupTimeout
	setupTimeout = 400 * time.Millisecond

	t.Cleanup(func() { setupTimeout = old })

	r.channels.On("Get", mock.Anything).Return(ari.NewChannelHandle(ari.NewKey(ari.ChannelKey, "caller-1"), r.channels, nil))
	r.channels.On("Answer", mock.Anything).Return(nil)
	r.channels.On("ExternalMedia", mock.Anything, mock.Anything).Return(nil, nil)

	bridges := &arimocks.Bridge{}
	bridges.On("Create", mock.Anything, "mixing", mock.Anything).Return(nil, assert.AnError) // bridging fails: the call is aborted by that, not by the timer
	r.m.cl.(*arimocks.Client).On("Bridge").Return(bridges)

	r.m.onStasisStart(context.Background(), &ari.StasisStart{Channel: ari.ChannelData{ID: "caller-1"}})

	r.m.mu.Lock()
	c := r.m.pending["caller-1"]
	r.m.mu.Unlock()
	require.NotNil(t, c)

	r.m.onStasisStart(context.Background(), &ari.StasisStart{Channel: ari.ChannelData{ID: c.ExternalID}})

	assert.Zero(t, r.m.SettingUp(), "out of pending: bridging was attempted")

	time.Sleep(700 * time.Millisecond) // past the timeout

	r.mu.Lock()
	defer r.mu.Unlock()

	assert.Len(t, r.hungUp, 2, "hung up once, by the failed bridge: the timer did not fire on top of it")
}

// A media channel whose call is already gone is hung up, not taken for a new caller.
func TestOnStasisStart_StrayMediaChannelIsHungUp(t *testing.T) {
	r := newOutboundRig(t)

	r.m.onStasisStart(context.Background(), &ari.StasisStart{Channel: ari.ChannelData{ID: "ext-9", Name: "AudioSocket/192.168.36.19:20101-x"}})

	r.channels.AssertCalled(t, "Hangup", mock.Anything, "")
	r.channels.AssertNotCalled(t, "Get", mock.Anything)
	assert.Zero(t, r.ports.InUse())
	assert.Equal(t, []string{"ext-9"}, r.hungUp)
}

func channelsJSON(now time.Time, rows ...[4]string) string {
	out := "["

	for i, r := range rows { // id, app_name, app_data, age in seconds
		age, _ := time.ParseDuration(r[3] + "s")
		if i > 0 {
			out += ","
		}

		out += `{"id":"` + r[0] + `","name":"AudioSocket/x","creationtime":"` + now.Add(-age).Format("2006-01-02T15:04:05.000-0700") +
			`","dialplan":{"app_name":"` + r[1] + `","app_data":"` + r[2] + `"}}`
	}

	return out + "]"
}

func TestSweep_HangsUpOnlyChannelsVaaniLostTrackOf(t *testing.T) {
	r := newOutboundRig(t)
	now := time.Now()

	tracked := &call{Call: session.New("known-1", "known-ext", 21000, session.Info{}, context.Background())}

	r.m.mu.Lock()
	r.m.pending["known-1"], r.m.pending["known-ext"] = tracked, tracked
	r.m.mu.Unlock()

	r.mu.Lock()
	r.reply = channelsJSON(now,
		[4]string{"orphan-media", "Stasis", "vaani,e763f874", "3000"},  // lost: hung up
		[4]string{"orphan-caller", "Stasis", "vaani", "200"},           // a caller whose StasisStart was lost: hung up
		[4]string{"orphan-out", "Stasis", "vaani,outbound,931", "100"}, // a dialed call that was lost: hung up
		[4]string{"known-ext", "Stasis", "vaani,aaaa", "3000"},         // Vaani tracks it
		[4]string{"young", "Stasis", "vaani,bbbb", "30"},               // setup may still be running
		[4]string{"other-app", "Stasis", "otherapp,cccc", "3000"},      // not ours
		[4]string{"vaanilike", "Stasis", "vaani2,dddd", "3000"},        // another app whose name starts the same
		[4]string{"in-dialplan", "AudioSocket", "uuid", "3000"},        // not in Stasis
	)
	r.mu.Unlock()

	got, err := r.m.sweepOnce(context.Background(), now)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{"orphan-media", "orphan-caller", "orphan-out"}, got)

	r.mu.Lock()
	defer r.mu.Unlock()

	assert.ElementsMatch(t, []string{"orphan-media", "orphan-caller", "orphan-out"}, r.hungUp)
}

func TestSweep_AnUnreadableListHangsUpNothing(t *testing.T) {
	r := newOutboundRig(t)

	r.mu.Lock()
	r.status = http.StatusInternalServerError
	r.mu.Unlock()

	_, err := r.m.sweepOnce(context.Background(), time.Now())
	assert.Error(t, err)
	assert.Empty(t, r.hungUp)
}

// A burst of callers whose setup is slow on Asterisk's side is handled in
// parallel (one call's answer and externalMedia requests don't wait for the
// others') and none is lost.
func TestRun_ABurstOfSlowSetupsIsHandledInParallel(t *testing.T) {
	const calls = 150

	channels := &arimocks.Channel{}
	channels.On("Hangup", mock.Anything, mock.Anything).Return(nil)
	channels.On("Get", mock.Anything).Return(func(k *ari.Key) *ari.ChannelHandle { return ari.NewChannelHandle(k, channels, nil) })
	channels.On("Answer", mock.Anything).Run(func(mock.Arguments) { time.Sleep(20 * time.Millisecond) }).Return(nil) // a slow Asterisk
	channels.On("ExternalMedia", mock.Anything, mock.Anything).Return(nil, nil)

	sub := &fakeSub{c: make(chan ari.Event, 100)}
	subscription := &arimocks.Subscription{}
	subscription.On("Events").Return((<-chan ari.Event)(sub.c))
	subscription.On("Cancel").Return()

	bus := &arimocks.Bus{}
	bus.On("Subscribe", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(subscription)

	cl := &arimocks.Client{}
	cl.On("Channel").Return(channels)
	cl.On("Bus").Return(bus)
	cl.On("Connected").Return(true)

	m := NewManager(cl, config.Config{AriApp: "vaani", MediaIP: "127.0.0.1"}, media.NewPortAllocator(22000, calls+10), nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() { defer close(done); m.Run(ctx) }()

	start := time.Now()

	for i := 0; i < calls; i++ {
		sub.c <- &ari.StasisStart{Channel: ari.ChannelData{ID: "caller-" + string(rune('A'+i/26)) + string(rune('a'+i%26))}}
	}

	require.Eventually(t, func() bool { return m.SettingUp() == calls }, 10*time.Second, 5*time.Millisecond, "every caller was staged")

	// Serially this is 150 x 20 ms = 3 s; 16 setups at a time it is ~0.2 s.
	assert.Less(t, time.Since(start), 2*time.Second)

	cancel()
	<-done
}
