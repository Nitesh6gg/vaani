package campaign

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/nitesh/vaani/internal/ari"
	"github.com/nitesh/vaani/internal/dograh"
)

// TestCall is one call to place outside any campaign, to try outbound calling
// before the campaign dialer runs (OUTBOUND_TEST_CALL).
type TestCall struct {
	Number     string
	OrgID      int
	WorkflowID int
}

// ParseTestCall reads "<number>,<org id>,<workflow id>".
func ParseTestCall(spec string) (TestCall, error) {
	bad := fmt.Errorf(`OUTBOUND_TEST_CALL must be "<number>,<org id>,<workflow id>", got %q`, spec)

	parts := strings.Split(spec, ",")
	if len(parts) != 3 {
		return TestCall{}, bad
	}

	org, err1 := strconv.Atoi(strings.TrimSpace(parts[1]))
	wf, err2 := strconv.Atoi(strings.TrimSpace(parts[2]))
	number := strings.TrimSpace(parts[0])

	if number == "" || err1 != nil || err2 != nil || org <= 0 || wf <= 0 {
		return TestCall{}, bad
	}

	return TestCall{Number: number, OrgID: org, WorkflowID: wf}, nil
}

// CallPlacer places a call; *ari.Manager is one.
type CallPlacer interface {
	Dial(ctx context.Context, req ari.DialRequest) (string, error)
}

// runStore is the part of dograh.Store a call needs.
type runStore interface {
	DialSettings(ctx context.Context, orgID int) (dograh.DialSettings, error)
	CreateOutboundRun(ctx context.Context, c dograh.Campaign, q dograh.QueuedRun, called, caller string) (int64, error)
	MarkNotConnected(ctx context.Context, runID int64, orgID int, status string) error
}

// RunTestCall dials tc the way a campaign call is dialed: the organization's
// dial settings, a Dograh run created first, the agent on answer, and a call
// that never connects completed in Dograh with its status. It returns once the
// call is placed; the outcome is logged when it arrives.
func RunTestCall(ctx context.Context, store runStore, d CallPlacer, tc TestCall, ring time.Duration) error {
	settings, err := store.DialSettings(ctx, tc.OrgID)
	if err != nil {
		return err
	}

	if settings.Provider != "ari" {
		return fmt.Errorf("organization %d's telephony provider is %q, not ari", tc.OrgID, settings.Provider)
	}

	caller := settings.CallerID(tc.WorkflowID)

	runID, err := store.CreateOutboundRun(ctx, dograh.Campaign{OrganizationID: tc.OrgID, WorkflowID: tc.WorkflowID},
		dograh.QueuedRun{}, tc.Number, caller)
	if err != nil {
		return err
	}

	_, err = d.Dial(ctx, ari.DialRequest{
		Endpoint: settings.Endpoint(tc.Number), CallerID: caller, Called: tc.Number, RingTimeout: ring,
		WorkflowID: tc.WorkflowID, RunID: runID,
		Done: func(out ari.Outcome) {
			if out.Connected {
				slog.Info("outbound test call finished", "event", "outbound.test_done", "run_id", runID, "connected", true)
				return
			}

			slog.Info("outbound test call did not connect", "event", "outbound.test_done", "run_id", runID,
				"connected", false, "status", out.Status, "cause", out.Cause)

			if err := store.MarkNotConnected(ctx, runID, tc.OrgID, out.Status); err != nil {
				slog.Error("outbound test call: completing its run failed", "run_id", runID, "error", err)
			}
		},
	})
	if err != nil {
		// Nothing was dialed: complete the run so it doesn't stay 'initialized'.
		return errors.Join(err, store.MarkNotConnected(ctx, runID, tc.OrgID, dograh.StatusFailed))
	}

	return nil
}
