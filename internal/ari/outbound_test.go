package ari

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
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
