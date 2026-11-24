package main

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
	Document    string   `json:"document"`
	Outcome     string   `json:"outcome"`
	Reason      string   `json:"reason,omitempty"`
	Status      int      `json:"http_status,omitempty"`
	CompletedAt string   `json:"server_completed_at,omitempty"`
	DelayMS     *float64 `json:"completed_observed_to_visible_ms"`
	CensoredMS  *float64 `json:"completed_observed_to_censor_ms"`
}

type visibilityStats struct {
	Selected int     `json:"selected"`
	Observed int     `json:"observed"`
	Censored int     `json:"censored"`
	Failed   int     `json:"failed"`
	Latency  latency `json:"observed_latency"`
}

type report struct {
	Target          string                 `json:"target"`
	Source          string                 `json:"source"`
	Backend         string                 `json:"backend"`
	HTTP            usage                  `json:"http_operations"`
	Queries         []sample               `json:"queries"`
	Visibility      []visibility           `json:"visibility,omitempty"`
	Lifecycle       []stepResult           `json:"lifecycle,omitempty"`
	VisibilityStats visibilityStats        `json:"visibility_summary"`
	Slices          map[string]sliceResult `json:"slices"`
}

func (r *report) summarize(live bool) {
	r.Slices = make(map[string]sliceResult)
	for group, samples := range byGroup(r.Queries) {
		r.Slices[group] = summarize(samples)
	}
	r.Source = "replay"
	if live {
		r.Source = "unmeasured"
		if r.Slices["all"].Results > 0 {
			r.Source = "hosted"
		}
	}
	r.VisibilityStats = visibilityStats{Selected: len(r.Visibility)}
	var durations []float64
	for _, v := range r.Visibility {
		switch v.Outcome {
		case "observed":
			r.VisibilityStats.Observed++
			if v.DelayMS != nil {
				durations = append(durations, *v.DelayMS)
			}
		case "censored":
			r.VisibilityStats.Censored++
		default:
			r.VisibilityStats.Failed++
		}
	}
	r.VisibilityStats.Latency = percentiles(durations)
}
