package main

import (
	"errors"
	"fmt"

	"github.com/Victoroide/quarry-rag/internal/evaluation"
)

type reviewKey struct {
	Target string `json:"target"`
	Query  string `json:"query"`
	Digest string `json:"response_sha256"`
}

type humanLabel struct {
	reviewKey
	Claims      *int  `json:"claims"`
	Unsupported *int  `json:"unsupported"`
	Accepted    *bool `json:"accepted"`
}

func annotate(o options) (output, error) {
	var out output
	if err := readJSON(o.Annotate, &out); err != nil {
		return output{}, fmt.Errorf("read prior report: %w", err)
	}
	if out.Version != evaluation.Version || len(out.Targets) == 0 {
		return output{}, errors.New("prior report fixture version does not match")
	}
	var labels []humanLabel
	if err := readJSON(o.Reviews, &labels); err != nil {
		return output{}, fmt.Errorf("read human labels: %w", err)
	}
	reviews := make(map[reviewKey]review, len(labels))
	for _, label := range labels {
		if label.Claims == nil || label.Unsupported == nil || label.Accepted == nil || *label.Claims < 0 ||
			*label.Claims > 1000 || *label.Unsupported < 0 || *label.Unsupported > *label.Claims {
			return output{}, errors.New("human labels require valid claims, unsupported counts and accepted decisions")
		}
		if _, duplicate := reviews[label.reviewKey]; duplicate {
			return output{}, errors.New("duplicate human label")
		}
		reviews[label.reviewKey] = review{Claims: *label.Claims, Unsupported: *label.Unsupported, Accepted: *label.Accepted}
	}
	for i := range out.Targets {
		target := &out.Targets[i]
		source := target.Source
		switch source {
		case "replay", "hosted", "unmeasured", "offline", "live_unverified":
		default:
			return output{}, errors.New("prior report has an invalid result source")
		}
		for j := range target.Queries {
			s := &target.Queries[j]
			key := reviewKey{Target: target.Target, Query: s.ID, Digest: s.Digest}
			if label, ok := reviews[key]; ok && s.Error == "" && s.Digest != "" {
				s.Review = &label
				delete(reviews, key)
			}
			if !o.IncludeRaw {
				s.Raw = nil
			}
		}
		target.summarize(source != "replay")
		// Offline scoring can downgrade legacy reports that lack provider
		// provenance. Only a live measurement can establish hosted provenance.
		if source != "hosted" {
			target.Source = source
		}
	}
	if len(reviews) != 0 {
		return output{}, errors.New("human label does not match a successful response in the report")
	}
	out.Settings.Raw = o.IncludeRaw
	out.compare()
	return out, nil
}
