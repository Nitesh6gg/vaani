package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Outbound campaign calls (internal/campaign). No campaign or organization
// labels: they would grow without bound; the call logs carry campaign_id.
var (
	CampaignDialsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_campaign_dials_total",
		Help: "Campaign calls placed (the dial was accepted by Asterisk).",
	})

	CampaignOutcomesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vaani_campaign_outcomes_total",
		Help: "How placed campaign calls ended: connected, busy, no-answer, rejected or failed (never connected, other cause).",
	}, []string{"status"})

	CampaignRetriesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_campaign_retries_total",
		Help: "Retries scheduled for busy and no-answer contacts.",
	})

	CampaignPausesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "vaani_campaign_pauses_total",
		Help: "Campaigns Vaani paused on its own, by reason: breaker (too many failed calls), wallet, provider (organization not on ARI).",
	}, []string{"reason"})

	CampaignsCompletedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "vaani_campaigns_completed_total",
		Help: "Campaigns Vaani completed: nothing queued, retried later or in flight.",
	})

	CampaignCallsInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "vaani_campaign_calls_in_flight",
		Help: "Campaign calls placed and not over yet (ringing or connected), all campaigns.",
	})
)
