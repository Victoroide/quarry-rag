package handler

import (
	"fmt"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/domain"
)

func TestProviderNeutralThrottling(t *testing.T) {
	code, retry, body := queryErrorResponse(fmt.Errorf("provider: %w", domain.ErrUpstreamThrottled))
	if code != 503 || retry != "2" || body.Error != "upstream_throttled" {
		t.Fatalf("unexpected response: %d %s %+v", code, retry, body)
	}
}
