package campaign

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nitesh/vaani/internal/ari"
	"github.com/nitesh/vaani/internal/dograh"
)

func TestParseTestCall(t *testing.T) {
	tc, err := ParseTestCall(" +919876543210 , 1, 19")
	require.NoError(t, err)
	assert.Equal(t, TestCall{Number: "+919876543210", OrgID: 1, WorkflowID: 19}, tc)

	for _, bad := range []string{"", "+91", "+91,1", "+91,x,19", "+91,1,0", ",1,19", "+91,1,19,5"} {
		_, err := ParseTestCall(bad)
		assert.Error(t, err, bad)
	}
}

type fakeStore struct {
	settings dograh.DialSettings
	runErr   error
	marked   chan string
}

func (f *fakeStore) DialSettings(context.Context, int) (dograh.DialSettings, error) {
	return f.settings, nil
}

func (f *fakeStore) CreateOutboundRun(context.Context, dograh.Campaign, dograh.QueuedRun, string, string) (int64, error) {
	return 931, f.runErr
}

func (f *fakeStore) MarkNotConnected(_ context.Context, _ int64, _ int, status string) error {
	f.marked <- status
	return nil
}

type fakeDialer struct {
	req ari.DialRequest
	err error
}

func (f *fakeDialer) Dial(_ context.Context, req ari.DialRequest) (string, error) {
	f.req = req
	return "chan-1", f.err
}

var ariSettings = dograh.DialSettings{Provider: "ari", Technology: "PJSIP", Trunk: "trunkA", FromNumbers: []string{"7317185732"}}

func TestRunTestCall_DialsLikeACampaignCall(t *testing.T) {
	st := &fakeStore{settings: ariSettings, marked: make(chan string, 1)}
	d := &fakeDialer{}

	require.NoError(t, RunTestCall(context.Background(), st, d, TestCall{Number: "9876543210", OrgID: 1, WorkflowID: 19}, 30*time.Second))

	assert.Equal(t, "PJSIP/9876543210@trunkA", d.req.Endpoint)
	assert.Equal(t, "7317185732", d.req.CallerID)
	assert.EqualValues(t, 931, d.req.RunID)
	assert.Equal(t, 19, d.req.WorkflowID)

	d.req.Done(ari.Outcome{Connected: true})
	assert.Empty(t, st.marked, "a connected call's run is completed by the agent")

	d.req.Done(ari.Outcome{Status: dograh.StatusBusy, Cause: 17})
	assert.Equal(t, dograh.StatusBusy, <-st.marked, "a call that never connected is completed with its status")
}

func TestRunTestCall_Refusals(t *testing.T) {
	other := ariSettings
	other.Provider = "twilio"

	err := RunTestCall(context.Background(), &fakeStore{settings: other}, &fakeDialer{}, TestCall{OrgID: 1, WorkflowID: 1}, time.Second)
	assert.ErrorContains(t, err, "not ari")

	err = RunTestCall(context.Background(), &fakeStore{settings: ariSettings, runErr: errors.New("db down")}, &fakeDialer{}, TestCall{OrgID: 1, WorkflowID: 1}, time.Second)
	assert.ErrorContains(t, err, "db down")

	st := &fakeStore{settings: ariSettings, marked: make(chan string, 1)}
	err = RunTestCall(context.Background(), st, &fakeDialer{err: errors.New("ari 500")}, TestCall{OrgID: 1, WorkflowID: 1}, time.Second)
	assert.ErrorContains(t, err, "ari 500")
	assert.Equal(t, dograh.StatusFailed, <-st.marked, "nothing dialed: the run is completed, not left initialized")
}
