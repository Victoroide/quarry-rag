package pinecone

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"sync/atomic"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/pinecone-io/go-pinecone/v5/pinecone"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const visibilityAPIVersion = "2026-07"

// visibilityPlane pins the unique host returned by DescribeIndex. A replacement
// index with the same name has its own host and cannot satisfy these receipts.
type visibilityPlane struct {
	endpoint url.URL
	apiKey   string
	http     http.Client
}

func newVisibilityPlane(idx *pinecone.Index, apiKey string) (*visibilityPlane, error) {
	if idx.Spec == nil || idx.Spec.Serverless == nil {
		return nil, errors.New("pinecone: INDEX_VISIBILITY_MODE=lsn requires a serverless index")
	}
	endpoint, err := url.Parse("https://" + idx.Host)
	if err != nil || endpoint.Hostname() == "" || endpoint.User != nil || endpoint.Path != "" || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" {
		return nil, errors.New("pinecone: invalid index host for visibility requests")
	}
	return &visibilityPlane{endpoint: *endpoint, apiKey: apiKey, http: http.Client{
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse },
	}}, nil
}

func parseVisibilityLSN(header http.Header, name string) (uint64, bool) {
	values := header.Values(name)
	if len(values) != 1 {
		return 0, false
	}
	value, err := strconv.ParseUint(values[0], 10, 64)
	return value, err == nil
}

type visibilityUpsert struct {
	Namespace string            `json:"namespace"`
	Vectors   []json.RawMessage `json:"vectors"`
}

func (p *Client) upsertForVisibility(ctx context.Context, namespace string, vectors []*pinecone.Vector) (domain.VisibilityProbe, error) {
	plane := p.visibility
	if namespace == "" {
		// Match the Go SDK so both modes write to the same namespace.
		namespace = "__default__"
	}
	batch := visibilityUpsert{Namespace: namespace, Vectors: make([]json.RawMessage, 0, p.batchSize)}
	envelope, err := json.Marshal(batch)
	if err != nil {
		return nil, err
	}
	var maxLSN uint64
	// Receipts bound the attempt only if every request that might have been
	// applied also returned one.
	completeReceipts := true
	flush := func() error {
		body, marshalErr := json.Marshal(batch)
		if marshalErr != nil {
			return marshalErr
		}
		var header http.Header
		if writeErr := p.mutate(ctx, false, func(attemptCtx context.Context) error {
			var ack struct {
				Count int `json:"upsertedCount"`
			}
			var postErr error
			header, postErr = plane.post(attemptCtx, "/vectors/upsert", body, &ack)
			if postErr != nil {
				if errors.Is(postErr, errDeliveryUncertain) {
					// The retry's receipt cannot cover this request if the index
					// applies it later. Keep retrying the write, but not the claim.
					completeReceipts = false
				}
				return postErr
			}
			if ack.Count != len(batch.Vectors) {
				return errors.New("pinecone: incomplete HTTP upsert acknowledgement")
			}
			return nil
		}); writeErr != nil {
			return writeErr
		}
		receipt, ok := parseVisibilityLSN(header, "x-pinecone-request-lsn")
		completeReceipts = completeReceipts && ok
		maxLSN = max(maxLSN, receipt)
		return nil
	}
	size := len(envelope)
	for _, vector := range vectors {
		encoded, encodeErr := json.Marshal(vector)
		if encodeErr != nil {
			return nil, fmt.Errorf("pinecone: encode vector for HTTP upsert: %w", encodeErr)
		}
		if len(envelope)+len(encoded) > maxUpsertBytes {
			return nil, errors.New("pinecone: vector exceeds the HTTP upsert byte limit")
		}
		if len(batch.Vectors) > 0 && (len(batch.Vectors) == p.batchSize || size+1+len(encoded) > maxUpsertBytes) {
			if flushErr := flush(); flushErr != nil {
				return nil, flushErr
			}
			clear(batch.Vectors)
			batch.Vectors = batch.Vectors[:0]
			size = len(envelope)
		}
		if len(batch.Vectors) > 0 {
			size++
		}
		batch.Vectors = append(batch.Vectors, encoded)
		size += len(encoded)
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if !completeReceipts {
		return nil, nil
	}
	return func(ctx context.Context, vector []float32) (domain.IndexVisibility, error) {
		queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
		defer cancel()
		return plane.checkWatermark(queryCtx, namespace, vector, maxLSN)
	}, nil
}

func (p *visibilityPlane) checkWatermark(ctx context.Context, namespace string, vector []float32, maxLSN uint64) (domain.IndexVisibility, error) {
	body, err := json.Marshal(struct {
		Namespace string    `json:"namespace"`
		Vector    []float32 `json:"vector"`
		TopK      int       `json:"topK"`
	}{namespace, vector, 1})
	if err != nil {
		return "", err
	}
	var reply struct {
		Namespace string `json:"namespace"`
	}
	header, err := p.post(ctx, "/query", body, &reply)
	if errors.Is(err, errVisibilityUnconfirmed) {
		return domain.VisibilityUnknown, nil
	}
	if err != nil {
		return "", err
	}
	watermark, ok := parseVisibilityLSN(header, "x-pinecone-max-indexed-lsn")
	if !ok || reply.Namespace != namespace {
		return domain.VisibilityUnknown, nil
	}
	if watermark >= maxLSN {
		return domain.VisibilityWritesIndexed, nil
	}
	return "", nil
}

var errVisibilityUnconfirmed = errors.New("pinecone: cannot confirm visibility against this endpoint")

// errDeliveryUncertain marks a failure after which the request may still be
// applied: it reached a connection and ended without a definite rejection.
var errDeliveryUncertain = errors.New("pinecone: request delivery is uncertain")

func (p *visibilityPlane) post(ctx context.Context, path string, body []byte, reply any) (http.Header, error) {
	endpoint := p.endpoint
	endpoint.Path = path
	// Without a connection nothing was sent, so a failure is a definite rejection.
	var connected atomic.Bool
	trace := &httptrace.ClientTrace{GotConn: func(httptrace.GotConnInfo) { connected.Store(true) }}
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodPost, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Api-Key", p.apiKey)
	req.Header.Set("X-Pinecone-Api-Version", visibilityAPIVersion)
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.http.Do(req)
	if err != nil {
		cause := status.Error(codes.Unavailable, "pinecone: HTTP data-plane request failed")
		if ctx.Err() != nil {
			cause = ctx.Err()
		}
		if connected.Load() {
			return nil, fmt.Errorf("%w: %w", errDeliveryUncertain, cause)
		}
		return nil, domain.RejectedVectorMutation(cause)
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusBadRequest:
		return nil, status.Error(codes.InvalidArgument, "pinecone: HTTP data-plane request rejected")
	case http.StatusUnauthorized:
		return nil, status.Error(codes.Unauthenticated, "pinecone: HTTP data-plane authentication failed")
	case http.StatusForbidden:
		return nil, status.Error(codes.PermissionDenied, "pinecone: HTTP data-plane request forbidden")
	case http.StatusNotFound, http.StatusGone, http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return nil, errVisibilityUnconfirmed
	case http.StatusTooManyRequests:
		return nil, domain.RejectedVectorMutation(status.Error(codes.ResourceExhausted, "pinecone: HTTP data-plane request throttled"))
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		// A gateway can answer for a request its origin goes on to apply.
		return nil, fmt.Errorf("%w: %w", errDeliveryUncertain, status.Errorf(codes.Unavailable, "pinecone: HTTP data plane returned %d", resp.StatusCode))
	default:
		return nil, fmt.Errorf("pinecone: HTTP data plane returned %d", resp.StatusCode)
	}
	const replyLimit = 64 << 10
	data, err := io.ReadAll(io.LimitReader(resp.Body, replyLimit+1))
	if err != nil {
		// The server already acknowledged the request. Do not retry the write
		// to retrieve a response body or receipt.
		return nil, errors.New("pinecone: could not read successful HTTP data-plane response")
	}
	if len(data) > replyLimit {
		return nil, errors.New("pinecone: HTTP data-plane response exceeds the byte limit")
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' || json.Unmarshal(data, reply) != nil {
		return nil, errors.New("pinecone: invalid HTTP data-plane response")
	}
	return resp.Header, nil
}
