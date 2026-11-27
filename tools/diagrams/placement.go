package main

import (
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
)

func decodePlacement(data []byte) (placement, error) {
	var p *placement
	err := jsonv2.Unmarshal(data, &p,
		jsonv2.RejectUnknownMembers(true),
		jsonv2.MatchCaseInsensitiveNames(true),
		json.MatchCaseSensitiveDelimiter(true),
	)
	if err != nil {
		return placement{}, fmt.Errorf("invalid placement JSON: %w", err)
	}
	if p == nil {
		return placement{}, errors.New("expected a placement object")
	}
	return *p, nil
}
