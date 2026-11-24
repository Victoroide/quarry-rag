package client

import (
	"context"
	"net/http"
	"net/url"
)

const evaluationFixtures = "/evaluation/fixtures"

// IngestEvaluationFixture submits a JSON document to the staged fixture endpoint.
func (c *Client) IngestEvaluationFixture(ctx context.Context, req IngestRequest, opts ...RequestOption) (*IngestResponse, error) {
	var response IngestResponse
	if err := c.jsonRequest(ctx, http.MethodPost, evaluationFixtures, req, &response, opts...); err != nil {
		return nil, err
	}
	return &response, nil
}

// GetEvaluationFixture looks up a receipt-qualified fixture revision.
func (c *Client) GetEvaluationFixture(ctx context.Context, id string, opts ...RequestOption) (*DocumentIncarnationSummary, error) {
	var response DocumentIncarnationSummary
	if err := c.jsonRequest(ctx, http.MethodGet, evaluationFixtures+"/"+url.PathEscape(id), nil, &response, opts...); err != nil {
		return nil, err
	}
	return &response, nil
}

// DeleteEvaluationFixture deletes a receipt-qualified fixture revision.
func (c *Client) DeleteEvaluationFixture(ctx context.Context, id string, opts ...RequestOption) (*DeleteResponse, error) {
	var response DeleteResponse
	if err := c.jsonRequest(ctx, http.MethodDelete, evaluationFixtures+"/"+url.PathEscape(id), nil, &response, opts...); err != nil {
		return nil, err
	}
	return &response, nil
}
