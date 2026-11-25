package client_test

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const syntheticService = "fixture-service-credential"

func syntheticSubject(n int32) string {
	return "e30.e30." + base64.RawURLEncoding.EncodeToString(fmt.Append(nil, n))
}

func TestSubjectProviderForEveryGoMethod(t *testing.T) {
	var calls, loads atomic.Int32
	type contextKey struct{}
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		assert.Equal(t, []string{syntheticService}, r.Header.Values("X-Service-Token"))
		assert.Equal(t, []string{"Bearer " + syntheticSubject(n)}, r.Header.Values("Authorization"))
		assert.NotContains(t, r.URL.String(), syntheticService)
		assert.NotContains(t, r.URL.String(), syntheticSubject(n))
		if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/") {
			file, _, err := r.FormFile("file")
			if assert.NoError(t, err) {
				assert.NoError(t, file.Close())
				assert.NoError(t, r.MultipartForm.RemoveAll())
			}
		}
		if r.Header.Get("Accept") == "text/event-stream" {
			w.Header().Set("Content-Type", "text/event-stream")
			_, err := io.WriteString(w, "event: context\ndata: {}\n\nevent: done\ndata: {}\n\n")
			assert.NoError(t, err)
			return
		}
		writeJSON(t, w, map[string]any{})
	}))
	t.Cleanup(service.Close)
	c, err := client.New(service.URL, client.WithServiceToken(syntheticService), client.WithSubjectCredentialProvider(func(ctx context.Context) (client.SubjectCredential, error) {
		assert.Equal(t, "caller", ctx.Value(contextKey{}))
		return client.SubjectCredential{Token: syntheticSubject(loads.Add(1)), ExpiresAt: time.Now().Add(time.Minute)}, nil
	}))
	require.NoError(t, err)
	ctx := context.WithValue(t.Context(), contextKey{}, "caller")
	const id = "00000000-0000-4000-8000-000000000001"
	_, err = c.Ingest(ctx, client.IngestRequest{})
	require.NoError(t, err)
	_, err = c.IngestFile(ctx, client.FileUpload{Reader: strings.NewReader("synthetic file"), Filename: "example.txt"})
	require.NoError(t, err)
	_, err = c.Query(ctx, client.QueryRequest{}, func(r *http.Request) {
		r.Header.Add("X-Service-Token", "caller header")
		r.Header.Add("Authorization", "caller header")
	})
	require.NoError(t, err)
	require.NoError(t, c.QueryStream(ctx, client.QueryRequest{}, func(client.Event) error { return nil }))
	_, err = c.ListDocuments(ctx, client.ListOptions{})
	require.NoError(t, err)
	_, err = c.ListDocumentsWithIncarnation(ctx, client.ListOptions{})
	require.NoError(t, err)
	_, err = c.GetDocument(ctx, id)
	require.NoError(t, err)
	_, err = c.GetDocumentWithIncarnation(ctx, id)
	require.NoError(t, err)
	_, err = c.DocumentFileURL(ctx, id)
	require.NoError(t, err)
	_, err = c.DeleteDocument(ctx, id)
	require.NoError(t, err)
	require.EqualValues(t, 10, calls.Load())
	require.EqualValues(t, 10, loads.Load())
}

func TestSubjectProviderFailureDoesNotSendRequests(t *testing.T) {
	var calls atomic.Int32
	service := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	t.Cleanup(service.Close)
	for _, provider := range []client.SubjectCredentialProvider{
		func(context.Context) (client.SubjectCredential, error) {
			return client.SubjectCredential{}, errors.New(syntheticService + syntheticSubject(1))
		},
		func(context.Context) (client.SubjectCredential, error) {
			return client.SubjectCredential{Token: syntheticSubject(1), ExpiresAt: time.Now().Add(-time.Second)}, nil
		},
		func(context.Context) (client.SubjectCredential, error) {
			return client.SubjectCredential{Token: syntheticSubject(1) + "\r\n", ExpiresAt: time.Now().Add(time.Minute)}, nil
		},
		func(context.Context) (client.SubjectCredential, error) {
			return client.SubjectCredential{Token: syntheticSubject(1)}, nil
		},
	} {
		c, err := client.New(service.URL, client.WithServiceToken(syntheticService), client.WithSubjectCredentialProvider(provider))
		require.NoError(t, err)
		_, err = c.Query(t.Context(), client.QueryRequest{})
		require.ErrorIs(t, err, client.ErrSubjectCredential)
		require.NotContains(t, err.Error(), syntheticService)
		require.NotContains(t, err.Error(), syntheticSubject(1))
	}
	require.Zero(t, calls.Load())
}

func TestSubjectProviderUsesRequestOptionContext(t *testing.T) {
	type contextKey struct{}
	for _, method := range []struct {
		name string
		call func(*client.Client, context.Context, client.RequestOption) error
	}{
		{"JSON", func(c *client.Client, ctx context.Context, opt client.RequestOption) error {
			_, err := c.Query(ctx, client.QueryRequest{}, opt)
			return err
		}},
		{"stream", func(c *client.Client, ctx context.Context, opt client.RequestOption) error {
			return c.QueryStream(ctx, client.QueryRequest{}, func(client.Event) error { return nil }, opt)
		}},
	} {
		t.Run(method.name, func(t *testing.T) {
			for _, tc := range []struct {
				name   string
				cancel bool
				want   error
			}{
				{"provider error", false, client.ErrSubjectCredential},
				{"canceled during refresh", true, context.Canceled},
			} {
				t.Run(tc.name, func(t *testing.T) {
					deadline := time.Now().Add(time.Minute)
					transportCalls := 0
					transport := subjectRoundTripper(func(*http.Request) (*http.Response, error) {
						transportCalls++
						return nil, errors.New("unexpected transport call")
					})
					var cancel context.CancelFunc
					endpoint := (&url.URL{Scheme: "http", Host: "localhost"}).String()
					c, err := client.New(endpoint, client.WithServiceToken(syntheticService),
						client.WithHTTPClient(&http.Client{Transport: transport}),
						client.WithSubjectCredentialProvider(func(ctx context.Context) (client.SubjectCredential, error) {
							received, ok := ctx.Deadline()
							assert.True(t, ok)
							assert.Equal(t, deadline, received)
							assert.Equal(t, "refresh", ctx.Value(contextKey{}))
							if tc.cancel {
								cancel()
								return client.SubjectCredential{Token: syntheticSubject(1), ExpiresAt: deadline}, nil
							}
							return client.SubjectCredential{}, errors.New("credential refresh failed")
						}))
					require.NoError(t, err)

					err = method.call(c, t.Context(), func(req *http.Request) {
						var derived context.Context
						derived, cancel = context.WithDeadline(context.WithValue(req.Context(), contextKey{}, "refresh"), deadline)
						t.Cleanup(cancel)
						*req = *req.WithContext(derived)
					})
					assert.ErrorIs(t, err, tc.want)
					require.Zero(t, transportCalls)
				})
			}
		})
	}
}

type subjectRoundTripper func(*http.Request) (*http.Response, error)

func (f subjectRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSubjectCredentialRedactionAndRedirects(t *testing.T) {
	subject := syntheticSubject(1)
	provider := client.WithSubjectCredentialProvider(func(context.Context) (client.SubjectCredential, error) {
		return client.SubjectCredential{Token: subject, ExpiresAt: time.Now().Add(time.Minute)}, nil
	})
	serviceToken := client.WithServiceToken(syntheticService)
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { targetCalls.Add(1) }))
	t.Cleanup(target.Close)
	service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer "+subject, r.Header.Get("Authorization"))
		assert.Equal(t, syntheticService, r.Header.Get("X-Service-Token"))
		w.Header().Set("X-Request-ID", subject)
		if r.Header.Get("Accept") == "text/event-stream" {
			w.Header().Set("Content-Type", "text/event-stream")
			_, err := fmt.Fprintf(w, "event: context\ndata: {}\n\nevent: error\ndata: {\"error\":%q,\"message\":%q}\n\n", subject, syntheticService)
			assert.NoError(t, err)
			return
		}
		w.Header().Set("Location", target.URL)
		w.WriteHeader(http.StatusTemporaryRedirect)
		writeJSON(t, w, map[string]string{"error": subject, "message": syntheticService + " " + subject, "code": syntheticService})
	}))
	t.Cleanup(service.Close)
	c, err := client.New(service.URL, provider, serviceToken)
	require.NoError(t, err)
	_, err = c.GetDocument(t.Context(), "sample")
	var apiErr *client.APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, http.StatusTemporaryRedirect, apiErr.StatusCode)
	require.NotContains(t, fmt.Sprintf("%+v", *apiErr), subject)
	require.NotContains(t, fmt.Sprintf("%+v", *apiErr), syntheticService)
	err = c.QueryStream(t.Context(), client.QueryRequest{}, func(event client.Event) error {
		require.NotContains(t, string(event.Data), subject)
		require.NotContains(t, string(event.Data), syntheticService)
		return nil
	})
	var streamErr *client.StreamError
	require.ErrorAs(t, err, &streamErr)
	require.NotContains(t, fmt.Sprintf("%+v", *streamErr), subject)
	require.NotContains(t, fmt.Sprintf("%+v", *streamErr), syntheticService)
	targetURL, err := url.Parse(target.URL)
	require.NoError(t, err)
	_, err = c.Query(t.Context(), client.QueryRequest{}, func(r *http.Request) { r.URL = targetURL })
	require.Error(t, err)
	require.Zero(t, targetCalls.Load())
	c, err = client.New(service.URL, provider, serviceToken, client.WithHTTPClient(&http.Client{Transport: subjectRoundTripper(func(r *http.Request) (*http.Response, error) {
		return nil, fmt.Errorf("transport failure: %s %s", r.Header.Get("Authorization"), r.Header.Get("X-Service-Token"))
	})}))
	require.NoError(t, err)
	_, err = c.GetDocument(t.Context(), "sample")
	require.Error(t, err)
	require.NotContains(t, err.Error(), subject)
	require.NotContains(t, err.Error(), syntheticService)
}
