package main

import (
	"slices"
	"time"

	"github.com/Victoroide/quarry-rag/internal/evaluation"
)

type usage struct {
	Cap           int  `json:"cap"`
	Used          int  `json:"used"`
	ProviderCalls *int `json:"provider_calls"`
}

type stepResult struct {
	Document string `json:"document"`
	Step     string `json:"step"`
	Outcome  string `json:"outcome"`
	Status   int    `json:"http_status,omitempty"`
}

type visibility struct {
	Document       string      `json:"document"`
	Outcome        string      `json:"outcome"`
	Reason         string      `json:"reason,omitempty"`
	Status         int         `json:"http_status,omitempty"`
	Completed      bool        `json:"completed"`
	CompletedAt    string      `json:"server_completed_at,omitempty"`
	FirstVisibleAt string      `json:"first_visible_observed_at,omitempty"`
	DelayMS        *float64    `json:"completed_observed_to_visible_ms"`
	CensoredMS     *float64    `json:"completed_observed_to_censor_ms"`
	EventClock     string      `json:"completion_event_clock"`
	EventDelayMS   *timeBounds `json:"completion_event_to_visible_ms"`
	EventCensorMS  *timeBounds `json:"completion_event_to_censor_ms"`
}

type timeBounds struct {
	Lower float64 `json:"lower"`
	Upper float64 `json:"upper"`
}

func (v *visibility) recordCompletionBounds(completed time.Time, proof *exchange, observed time.Time) {
	v.EventClock = "unmeasured"
	dbTime, err := time.Parse(time.RFC3339Nano, proof.databaseTime)
	if err != nil || dbTime.Before(completed) || proof.started.IsZero() || observed.Before(proof.started) {
		return
	}
	v.EventClock = "postgres_age_plus_client_monotonic"
	age, window := milliseconds(dbTime.Sub(completed)), milliseconds(observed.Sub(proof.started))
	if v.DelayMS != nil {
		lower := age + *v.DelayMS
		v.EventDelayMS = &timeBounds{Lower: lower, Upper: lower + window}
	}
	if v.CensoredMS != nil {
		lower := age + *v.CensoredMS
		v.EventCensorMS = &timeBounds{Lower: lower, Upper: lower + window}
	}
}

type visibilityStats struct {
	Selected   int     `json:"selected"`
	Observed   int     `json:"observed"`
	Censored   int     `json:"censored"`
	Failed     int     `json:"failed"`
	Latency    latency `json:"observed_latency"`
	EventLower latency `json:"completion_event_lower_latency"`
	EventUpper latency `json:"completion_event_upper_latency"`
}

type report struct {
	Target          string                 `json:"target"`
	Source          string                 `json:"source"`
	Backend         string                 `json:"backend"`
	Deployment      *deployment            `json:"deployment,omitempty"`
	HTTP            usage                  `json:"http_operations"`
	Queries         []sample               `json:"queries"`
	Visibility      []visibility           `json:"visibility,omitempty"`
	Lifecycle       []stepResult           `json:"lifecycle,omitempty"`
	VisibilityStats visibilityStats        `json:"visibility_summary"`
	Slices          map[string]sliceResult `json:"slices"`
	Ledger          string                 `json:"cleanup_ledger,omitempty"`
	RemainingIDs    []string               `json:"cleanup_ids,omitempty"`
}

func (r *report) summarize(live bool) {
	r.Slices = make(map[string]sliceResult)
	for group, samples := range byGroup(r.Queries) {
		r.Slices[group] = summarize(samples)
	}
	r.Source = "replay"
	if live {
		r.Source = r.liveSource()
	}
	r.VisibilityStats = visibilityStats{Selected: len(r.Visibility)}
	var durations, eventLower, eventUpper []float64
	for i := range r.Visibility {
		v := &r.Visibility[i]
		if v.EventClock == "" {
			v.EventClock = "unmeasured"
		}
		switch v.Outcome {
		case "observed":
			r.VisibilityStats.Observed++
			if v.DelayMS != nil {
				durations = append(durations, *v.DelayMS)
			}
			if v.EventDelayMS != nil {
				eventLower = append(eventLower, v.EventDelayMS.Lower)
				eventUpper = append(eventUpper, v.EventDelayMS.Upper)
			}
		case "censored":
			r.VisibilityStats.Censored++
		default:
			r.VisibilityStats.Failed++
		}
	}
	r.VisibilityStats.Latency = percentiles(durations)
	r.VisibilityStats.EventLower = percentiles(eventLower)
	r.VisibilityStats.EventUpper = percentiles(eventUpper)
}

func (r *report) liveSource() string {
	if r.Deployment != nil && (r.Deployment.AnswerProvider == "offline" || r.Deployment.EmbeddingProvider == "offline") ||
		slices.ContainsFunc(r.Queries, func(s sample) bool { return s.Execution == "offline" }) {
		return "offline"
	}
	if r.Slices["all"].Results == 0 {
		return "unmeasured"
	}
	if r.Deployment != nil {
		execution := evaluation.ProviderExecution(r.Backend, r.Deployment.EmbeddingProvider, r.Deployment.AnswerProvider)
		if execution != "" && slices.ContainsFunc(r.Queries, func(s sample) bool {
			return s.Error == "" && !s.Empty && !s.NoEvidence && s.Execution == execution &&
				evaluation.ValidUpstreams(s.Upstreams, r.Deployment.EmbeddingProvider, r.Deployment.AnswerProvider)
		}) {
			return "hosted"
		}
	}
	return "live_unverified"
}
