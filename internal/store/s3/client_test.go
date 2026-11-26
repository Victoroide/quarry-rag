package s3

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

func s3EndpointClient(t *testing.T, endpoint, owner string) *Client {
	t.Helper()
	return newClient(aws.Config{
		Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""), RetryMaxAttempts: 1,
	}, "owned-bucket", owner, func(o *awss3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})
}

type httpClientFunc func(*http.Request) (*http.Response, error)

func (f httpClientFunc) Do(req *http.Request) (*http.Response, error) { return f(req) }

func retryingClient(t *testing.T, endpoint string, maxAttempts int, httpClient aws.HTTPClient, backoff ...time.Duration) *Client {
	t.Helper()
	delay := time.Duration(0)
	if len(backoff) > 0 {
		delay = backoff[0]
	}
	return retryingClientWithCreds(t, endpoint, maxAttempts, credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""), httpClient, delay)
}

func retryingClientWithCreds(t *testing.T, endpoint string, maxAttempts int, creds aws.CredentialsProvider, httpClient aws.HTTPClient, delay time.Duration) *Client {
	t.Helper()
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: creds,
		HTTPClient:  httpClient,
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = maxAttempts
				o.RateLimiter = ratelimit.None
				o.Backoff = retry.BackoffDelayerFunc(func(int, error) (time.Duration, error) {
					return delay, nil
				})
			})
		},
	}
	return newClient(cfg, "owned-bucket", "", func(o *awss3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = &endpoint
		}
		o.UsePathStyle = true
	})
}

func closedListenerAddr(t *testing.T) string {
	t.Helper()
	lc := net.ListenConfig{}
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func writeS3Error(w http.ResponseWriter, status int, code, requestID string) {
	if requestID != "" {
		w.Header().Set("X-Amz-Request-Id", requestID)
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>%s</Message><RequestId>body-req-id</RequestId></Error>`, code, code)
}

func hijackAndClose(t *testing.T, w http.ResponseWriter, rst bool) {
	t.Helper()
	conn, _, err := http.NewResponseController(w).Hijack()
	if err != nil {
		t.Error(err)
		return
	}
	if rst {
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
	}
	_ = conn.Close()
}

func TestPutSourceSeparatesAcknowledgmentFromSettlement(t *testing.T) {
	for _, retry := range []bool{false, true} {
		name := "first attempt"
		if retry {
			name = "retry after gateway timeout"
		}
		t.Run(name, func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					t.Error(err)
				}
				if attempts.Add(1) == 1 && retry {
					w.WriteHeader(http.StatusGatewayTimeout)
					return
				}
				w.Header().Set("x-amz-version-id", "written")
			}))
			defer server.Close()
			client := newClient(aws.Config{
				Region: "us-east-1", Credentials: credentials.NewStaticCredentialsProvider("test-key", "test-secret", ""), RetryMaxAttempts: 2,
			}, "owned-bucket", "", func(o *awss3.Options) {
				o.BaseEndpoint = &server.URL
				o.UsePathStyle = true
			})
			result, err := client.PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("original"))
			if err != nil || !result.Acknowledged || result.VersionID != "written" || result.Settled == retry {
				t.Fatalf("unexpected outcome after %d attempts: %+v, %v", attempts.Load(), result, err)
			}
		})
	}
}

func TestS3RequestsCarryExpectedOwner(t *testing.T) {
	for _, owner := range []string{"123456789012", ""} {
		t.Run(owner, func(t *testing.T) {
			methods := make(chan string, 3)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				methods <- r.Method
				if got := r.Header.Get("x-amz-expected-bucket-owner"); got != owner {
					t.Errorf("%s owner = %q, want %q", r.Method, got, owner)
				}
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					t.Error(err)
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			client := s3EndpointClient(t, server.URL, owner)
			key, err := client.Upload(t.Context(), "id", "document", "file.pdf", extractor.FileTypePDF, strings.NewReader("content"))
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Delete(t.Context(), key); err != nil {
				t.Fatal(err)
			}
			if err := client.Ping(t.Context()); err != nil {
				t.Fatal(err)
			}
			if got := strings.Join([]string{<-methods, <-methods, <-methods}, ","); got != "PUT,DELETE,HEAD" {
				t.Fatalf("requests = %s", got)
			}
		})
	}
}

func TestPresignForcesAttachment(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	client := s3EndpointClient(t, server.URL, "123456789012")
	for _, file := range []struct{ name, contentType string }{{"report.pdf", "application/pdf"}, {"notes.txt", "text/plain; charset=utf-8"}, {"résumé.docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"}, {"unknown.unknown", "application/octet-stream"}} {
		t.Run(file.name, func(t *testing.T) {
			raw, err := client.PresignGet(t.Context(), "documents/document/id/"+file.name)
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := url.Parse(raw)
			if err != nil {
				t.Fatal(err)
			}
			query := parsed.Query()
			disposition, params, err := mime.ParseMediaType(query.Get("response-content-disposition"))
			if err != nil || disposition != "attachment" || params["filename"] != file.name {
				t.Errorf("download disposition = %q, error = %v", query.Get("response-content-disposition"), err)
			}
			if query.Get("response-content-type") != file.contentType {
				t.Errorf("content type = %q, want %q", query.Get("response-content-type"), file.contentType)
			}
			for name, values := range query {
				if strings.Contains(strings.ToLower(name), "expected-bucket-owner") || (strings.EqualFold(name, "X-Amz-SignedHeaders") && strings.Contains(strings.Join(values, ";"), "expected-bucket-owner")) {
					t.Errorf("download requires owner header: %s", name)
				}
			}
		})
	}
}

func TestPresignRequestsFullLifetimeWithTemporaryCredentials(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("presigning contacted the S3 endpoint")
		w.WriteHeader(http.StatusBadGateway)
	}))
	t.Cleanup(server.Close)
	client := newClient(aws.Config{
		Region: "us-east-1",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     "fixture-key",
				SecretAccessKey: "fixture-secret",
				SessionToken:    "fixture-session",
				CanExpire:       true,
				Expires:         time.Now().Add(time.Minute),
			}, nil
		}),
	}, "fixture-bucket", "", func(o *awss3.Options) {
		o.BaseEndpoint = &server.URL
		o.UsePathStyle = true
	})
	address, err := client.PresignGet(t.Context(), "documents/report.pdf")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(address)
	if err != nil {
		t.Fatal("expected a parseable download address")
	}
	if got := parsed.Query().Get("X-Amz-Expires"); got != "900" {
		t.Errorf("requested expiry = %q, want 900", got)
	}
}

func TestPresignWithTTLBoundsAndAppliesExpiry(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	client := s3EndpointClient(t, server.URL, "")
	for _, bad := range []time.Duration{-time.Second, 0, 999 * time.Millisecond, 15*time.Minute + time.Second} {
		if _, err := client.PresignGetWithTTL(t.Context(), "documents/report.pdf", bad); err == nil {
			t.Fatalf("expected error for TTL %s", bad)
		}
	}
	for _, seconds := range []int{1, 45, 900} {
		raw, err := client.PresignGetWithTTL(t.Context(), "documents/report.pdf", time.Duration(seconds)*time.Second)
		if err != nil {
			t.Fatalf("TTL %ds: %v", seconds, err)
		}
		parsed, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("parse URL: %v", err)
		}
		if got := parsed.Query().Get("X-Amz-Expires"); got != strconv.Itoa(seconds) {
			t.Errorf("TTL %ds: X-Amz-Expires = %q", seconds, got)
		}
	}
}

func TestVerifyMapsAccessDeniedToConfigurationError(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusForbidden, http.StatusNotFound, http.StatusInternalServerError} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodHead || r.Header.Get("x-amz-expected-bucket-owner") != "123456789012" {
					t.Errorf("verification request = %s, owner = %q", r.Method, r.Header.Get("x-amz-expected-bucket-owner"))
				}
				w.WriteHeader(code)
			}))
			defer server.Close()
			client := s3EndpointClient(t, server.URL, "123456789012")
			err := client.Verify(t.Context())
			if code == http.StatusOK {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("inaccessible bucket accepted")
			}
			unavailable := code == http.StatusForbidden || code == http.StatusNotFound
			if errors.Is(err, ErrBucketUnavailable) != unavailable {
				t.Errorf("bucket error classification = %v", err)
			}
			var responseErr *awshttp.ResponseError
			if !errors.As(err, &responseErr) || responseErr.HTTPStatusCode() != code {
				t.Errorf("underlying response error lost: %v", err)
			}
		})
	}
}

func TestUploadSanitizesKey(t *testing.T) {
	received := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- strings.TrimPrefix(r.URL.Path, "/owned-bucket/")
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client := s3EndpointClient(t, server.URL, "")
	for _, tt := range []struct{ name, docType, filename, wantType, wantFile string }{
		{"separators", " ../sales\\team\n. ", " ../report\\part\x00.pdf. ", "_sales_team_", "_report_part_.pdf"},
		{"empty", " .. ", ". .", "document", "file"},
		{"bounded unicode", strings.Repeat("界", 30), strings.Repeat("é", 110), strings.Repeat("界", 21), strings.Repeat("é", 100)},
		{"plain", "reference", "report.pdf", "reference", "report.pdf"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			key, err := client.Upload(t.Context(), "id", tt.docType, tt.filename, extractor.FileTypePDF, strings.NewReader("content"))
			if err != nil {
				t.Fatal(err)
			}
			want := "documents/" + tt.wantType + "/id/" + tt.wantFile
			stored := <-received
			if key != want || stored != want || !utf8.ValidString(key) {
				t.Fatalf("key = %q, stored key = %q, want %q", key, stored, want)
			}
		})
	}
}

func TestNewKeyIsAllocatedBeforeTheWrite(t *testing.T) {
	received := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received <- strings.TrimPrefix(r.URL.Path, "/owned-bucket/")
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	client := s3EndpointClient(t, server.URL, "")
	key := client.NewKey("11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222", "reference", "../report.pdf")
	if want := "documents/reference/11111111-1111-4111-8111-111111111111/22222222-2222-4222-8222-222222222222/_report.pdf"; key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}
	if other := client.NewKey("11111111-1111-4111-8111-111111111111", "33333333-3333-4333-8333-333333333333", "reference", "../report.pdf"); other == key {
		t.Fatal("attempts share an object key")
	}
	select {
	case got := <-received:
		t.Fatalf("allocating a key wrote %q", got)
	default:
	}
	if client.Bucket() != "owned-bucket" {
		t.Fatalf("bucket = %q", client.Bucket())
	}
	if _, err := client.Put(t.Context(), key, extractor.FileTypePDF, strings.NewReader("content")); err != nil {
		t.Fatal(err)
	}
	if got := <-received; got != key {
		t.Fatalf("stored key = %q, want %q", got, key)
	}
}

func TestPutReportsTheCreatedVersion(t *testing.T) {
	for _, version := range []string{"", "3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY"} {
		t.Run("version="+version, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					t.Error(err)
				}
				if version != "" {
					w.Header().Set("x-amz-version-id", version)
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			got, err := s3EndpointClient(t, server.URL, "").Put(t.Context(), "documents/document/id/attempt/file.pdf", extractor.FileTypePDF, strings.NewReader("content"))
			if err != nil || got != version {
				t.Fatalf("version = %q, error = %v, want %q", got, err, version)
			}
		})
	}
}

func TestPutFailureIsReported(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error(err)
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	if _, err := s3EndpointClient(t, server.URL, "").Put(t.Context(), "documents/document/id/attempt/file.pdf", extractor.FileTypePDF, strings.NewReader("content")); err == nil {
		t.Fatal("failed write accepted")
	}
}

func TestDeleteVersionTargetsOneVersion(t *testing.T) {
	for _, version := range []string{"v1", "null"} {
		t.Run("version="+version, func(t *testing.T) {
			requests := make(chan *http.Request, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- r
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()
			client := s3EndpointClient(t, server.URL, "123456789012")
			if err := client.DeleteVersion(t.Context(), "documents/document/id/attempt/file.pdf", version); err != nil {
				t.Fatal(err)
			}
			r := <-requests
			if r.Method != http.MethodDelete || r.URL.Path != "/owned-bucket/documents/document/id/attempt/file.pdf" {
				t.Fatalf("request = %s %s", r.Method, r.URL.Path)
			}
			if got := r.URL.Query().Get("versionId"); got != version {
				t.Fatalf("versionId = %q, want %q", got, version)
			}
			if got := r.Header.Get("x-amz-expected-bucket-owner"); got != "123456789012" {
				t.Fatalf("owner = %q", got)
			}
		})
	}
}

func TestDeleteVersionRequiresAnID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := s3EndpointClient(t, server.URL, "")
	if err := client.DeleteVersion(t.Context(), "documents/a/key.pdf", ""); !errors.Is(err, ErrVersionIDMissing) {
		t.Fatalf("error = %v, want ErrVersionIDMissing", err)
	}
}

const versionListing = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult>
  <Name>owned-bucket</Name><Prefix>documents/a/key.pdf</Prefix><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>
  <DeleteMarker><Key>documents/a/key.pdf</Key><VersionId>marker</VersionId><IsLatest>true</IsLatest></DeleteMarker>
  <Version><Key>documents/a/key.pdf</Key><VersionId>second</VersionId><IsLatest>false</IsLatest><Size>20</Size></Version>
  <Version><Key>documents/a/key.pdf</Key><VersionId>null</VersionId><IsLatest>false</IsLatest><Size>10</Size></Version>
  <Version><Key>documents/a/key.pdf.bak</Key><VersionId>other</VersionId><IsLatest>true</IsLatest><Size>99</Size></Version>
</ListVersionsResult>`

func TestListVersionsReturnsOnlyTheExactKey(t *testing.T) {
	requests := make(chan *http.Request, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r
		w.Header().Set("Content-Type", "application/xml")
		_, _ = io.WriteString(w, versionListing)
	}))
	defer server.Close()
	client := s3EndpointClient(t, server.URL, "123456789012")
	versions, complete, err := client.ListVersions(t.Context(), "documents/a/key.pdf", 10)
	if err != nil || !complete {
		t.Fatalf("complete = %v, error = %v", complete, err)
	}
	got := map[string]ObjectVersion{}
	for _, v := range versions {
		got[v.ID] = v
	}
	want := map[string]ObjectVersion{
		"marker": {ID: "marker", DeleteMarker: true},
		"second": {ID: "second", Size: 20},
		"null":   {ID: "null", Size: 10},
	}
	if len(got) != len(want) || got["marker"] != want["marker"] || got["second"] != want["second"] || got["null"] != want["null"] {
		t.Fatalf("versions = %+v, want %+v", got, want)
	}
	r := <-requests
	if r.Method != http.MethodGet || !r.URL.Query().Has("versions") || r.URL.Query().Get("prefix") != "documents/a/key.pdf" {
		t.Fatalf("request = %s %s", r.Method, r.URL.String())
	}
	if r.Header.Get("x-amz-expected-bucket-owner") != "123456789012" {
		t.Fatalf("owner = %q", r.Header.Get("x-amz-expected-bucket-owner"))
	}
}

func TestListVersionsFollowsPagesAndBoundsTheResult(t *testing.T) {
	page := func(marker string, truncated bool, ids ...string) string {
		var body strings.Builder
		body.WriteString(`<ListVersionsResult><Name>owned-bucket</Name>`)
		if truncated {
			body.WriteString("<IsTruncated>true</IsTruncated><NextKeyMarker>documents/a/key.pdf</NextKeyMarker><NextVersionIdMarker>" + marker + "</NextVersionIdMarker>")
		} else {
			body.WriteString("<IsTruncated>false</IsTruncated>")
		}
		for _, id := range ids {
			body.WriteString("<Version><Key>documents/a/key.pdf</Key><VersionId>" + id + "</VersionId><IsLatest>false</IsLatest><Size>1</Size></Version>")
		}
		body.WriteString("</ListVersionsResult>")
		return body.String()
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Get("version-id-marker") == "v2" {
			_, _ = io.WriteString(w, page("", false, "v3"))
			return
		}
		_, _ = io.WriteString(w, page("v2", true, "v1", "v2"))
	}))
	defer server.Close()
	client := s3EndpointClient(t, server.URL, "")
	versions, complete, err := client.ListVersions(t.Context(), "documents/a/key.pdf", 10)
	if err != nil || !complete || len(versions) != 3 {
		t.Fatalf("versions = %+v, complete = %v, error = %v", versions, complete, err)
	}
	versions, complete, err = client.ListVersions(t.Context(), "documents/a/key.pdf", 2)
	if err != nil || complete || len(versions) != 2 {
		t.Fatalf("bounded versions = %+v, complete = %v, error = %v", versions, complete, err)
	}
}

func TestListVersionsReportsFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) }))
	defer server.Close()
	if _, _, err := s3EndpointClient(t, server.URL, "").ListVersions(t.Context(), "documents/a/key.pdf", 10); err == nil {
		t.Fatal("denied listing accepted")
	}
}

func TestVersionLockReadsRetentionAndLegalHold(t *testing.T) {
	until := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	for name, tc := range map[string]struct {
		headers map[string]string
		status  int
		want    ObjectLock
		missing bool
		failed  bool
	}{
		"unlocked":  {status: http.StatusOK},
		"legal":     {status: http.StatusOK, headers: map[string]string{"x-amz-object-lock-legal-hold": "ON"}, want: ObjectLock{LegalHold: true}},
		"released":  {status: http.StatusOK, headers: map[string]string{"x-amz-object-lock-legal-hold": "OFF"}},
		"retention": {status: http.StatusOK, headers: map[string]string{"x-amz-object-lock-mode": "COMPLIANCE", "x-amz-object-lock-retain-until-date": until.Format(time.RFC3339)}, want: ObjectLock{RetainUntil: until}},
		"missing":   {status: http.StatusNotFound, missing: true},
		"denied":    {status: http.StatusForbidden, failed: true},
	} {
		t.Run(name, func(t *testing.T) {
			requests := make(chan *http.Request, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests <- r
				for key, value := range tc.headers {
					w.Header().Set(key, value)
				}
				w.WriteHeader(tc.status)
			}))
			defer server.Close()
			lock, err := s3EndpointClient(t, server.URL, "").VersionLock(t.Context(), "documents/a/key.pdf", "v1")
			r := <-requests
			if r.Method != http.MethodHead || r.URL.Query().Get("versionId") != "v1" {
				t.Fatalf("request = %s %s", r.Method, r.URL.String())
			}
			switch {
			case tc.missing:
				if !errors.Is(err, ErrVersionNotFound) {
					t.Fatalf("error = %v, want ErrVersionNotFound", err)
				}
			case tc.failed:
				if err == nil || errors.Is(err, ErrVersionNotFound) {
					t.Fatalf("error = %v, want a lookup failure", err)
				}
			default:
				if err != nil || lock.LegalHold != tc.want.LegalHold || !lock.RetainUntil.Equal(tc.want.RetainUntil) {
					t.Fatalf("lock = %+v, error = %v, want %+v", lock, err, tc.want)
				}
			}
		})
	}
}
func TestPutSourceAccountsForEachAttempt(t *testing.T) {
	t.Run("refused_403", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			writeS3Error(w, http.StatusForbidden, "AccessDenied", "req-403")
		}))
		defer server.Close()

		result, err := retryingClient(t, server.URL, 3, nil).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err == nil || result.Acknowledged || !result.Settled || calls.Load() != 1 {
			t.Fatalf("got result=%+v err=%v calls=%d, want err, Acknowledged=false, Settled=true, calls=1", result, err, calls.Load())
		}
	})

	t.Run("refused_404", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "req-404")
		}))
		defer server.Close()

		result, err := retryingClient(t, server.URL, 3, nil).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err == nil || result.Acknowledged || !result.Settled {
			t.Fatalf("got result=%+v err=%v, want err, Acknowledged=false, Settled=true", result, err)
		}
	})

	t.Run("refused_400", func(t *testing.T) {
		for _, code := range []string{"InvalidArgument", "KMS.DisabledException"} {
			t.Run(code, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					writeS3Error(w, http.StatusBadRequest, code, "req-400")
				}))
				defer server.Close()

				result, err := retryingClient(t, server.URL, 3, nil).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
				if err == nil || result.Acknowledged || !result.Settled {
					t.Fatalf("got result=%+v err=%v, want err, Acknowledged=false, Settled=true", result, err)
				}
			})
		}
	})

	t.Run("refused_redirect", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			writeS3Error(w, http.StatusMovedPermanently, "PermanentRedirect", "req-301")
		}))
		defer server.Close()

		result, err := retryingClient(t, server.URL, 3, nil).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err == nil || result.Acknowledged || !result.Settled {
			t.Fatalf("got result=%+v err=%v, want err, Acknowledged=false, Settled=true", result, err)
		}
	})

	t.Run("refusal_without_request_id", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			writeS3Error(w, http.StatusForbidden, "AccessDenied", "")
		}))
		defer server.Close()

		result, err := retryingClient(t, server.URL, 3, nil).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err == nil || result.Acknowledged || result.Settled {
			t.Fatalf("got result=%+v err=%v, want err, Acknowledged=false, Settled=false", result, err)
		}
	})

	t.Run("refused_before_body", func(t *testing.T) {
		var sawExpect atomic.Bool
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if strings.EqualFold(r.Header.Get("Expect"), "100-continue") {
				sawExpect.Store(true)
			}
			writeS3Error(w, http.StatusForbidden, "AccessDenied", "req-403-early")
		}))
		defer server.Close()

		body := bytes.NewReader(bytes.Repeat([]byte("a"), 3<<20))
		result, err := retryingClient(t, server.URL, 3, nil).PutSource(t.Context(), "documents/large.bin", extractor.FileTypeText, body)
		if err == nil || result.Acknowledged || !result.Settled || !sawExpect.Load() {
			t.Fatalf("got result=%+v err=%v, want err, Acknowledged=false, Settled=true (expect100=%v)", result, err, sawExpect.Load())
		}
	})

	t.Run("refused_then_acknowledged", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			if calls.Add(1) == 1 {
				writeS3Error(w, http.StatusBadRequest, "RequestTimeout", "req-400")
				return
			}
			w.Header().Set("X-Amz-Version-Id", "v2")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		result, err := retryingClient(t, server.URL, 3, nil).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err != nil || !result.Acknowledged || !result.Settled || result.VersionID != "v2" || calls.Load() != 2 {
			t.Fatalf("got result=%+v err=%v calls=%d, want Acknowledged=true, Settled=true, v2, calls=2", result, err, calls.Load())
		}
	})

	t.Run("unknown_then_acknowledged", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			if calls.Add(1) == 1 {
				writeS3Error(w, http.StatusBadRequest, "RequestTimeout", "")
				return
			}
			w.Header().Set("X-Amz-Version-Id", "v2")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		result, err := retryingClient(t, server.URL, 3, nil).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err != nil || !result.Acknowledged || result.Settled || result.VersionID != "v2" || calls.Load() != 2 {
			t.Fatalf("got result=%+v err=%v calls=%d, want Acknowledged=true, Settled=false, v2, calls=2", result, err, calls.Load())
		}
	})

	t.Run("server_error_then_acknowledged", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			if calls.Add(1) == 1 {
				writeS3Error(w, http.StatusServiceUnavailable, "SlowDown", "req-503")
				return
			}
			w.Header().Set("X-Amz-Version-Id", "v2")
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		result, err := retryingClient(t, server.URL, 3, nil).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err != nil || !result.Acknowledged || result.Settled || result.VersionID != "v2" || calls.Load() != 2 {
			t.Fatalf("got result=%+v err=%v calls=%d, want Acknowledged=true, Settled=false, v2, calls=2", result, err, calls.Load())
		}
	})

	t.Run("server_errors", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			writeS3Error(w, http.StatusInternalServerError, "InternalError", "req-500")
		}))
		defer server.Close()

		result, err := retryingClient(t, server.URL, 3, nil).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err == nil || result.Acknowledged || result.Settled || calls.Load() != 3 {
			t.Fatalf("got result=%+v err=%v calls=%d, want err, Acknowledged=false, Settled=false, calls=3", result, err, calls.Load())
		}
	})

	t.Run("connection_refused", func(t *testing.T) {
		endpoint := "http://" + closedListenerAddr(t)
		result, err := retryingClient(t, endpoint, 3, nil).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err == nil || result.Acknowledged || !result.Settled {
			t.Fatalf("got result=%+v err=%v, want err, Acknowledged=false, Settled=true", result, err)
		}
	})

	t.Run("tls_handshake", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}}}
		result, err := retryingClient(t, server.URL, 3, httpClient).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err == nil || result.Acknowledged || !result.Settled || calls.Load() != 0 {
			t.Fatalf("got result=%+v err=%v calls=%d, want err, Acknowledged=false, Settled=true, calls=0", result, err, calls.Load())
		}
	})

	t.Run("proxy_unreachable", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		proxyURL, err := url.Parse("http://" + closedListenerAddr(t))
		if err != nil {
			t.Fatal(err)
		}
		httpClient := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
		result, err := retryingClient(t, server.URL, 3, httpClient).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err == nil || result.Acknowledged || !result.Settled || calls.Load() != 0 {
			t.Fatalf("got result=%+v err=%v calls=%d, want err, Acknowledged=false, Settled=true, calls=0", result, err, calls.Load())
		}
	})

	t.Run("credentials_unavailable", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			calls.Add(1)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		creds := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{}, errors.New("no credentials")
		})
		result, err := retryingClientWithCreds(t, server.URL, 3, creds, nil, 0).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err == nil || result.Acknowledged || !result.Settled || calls.Load() != 0 {
			t.Fatalf("got result=%+v err=%v calls=%d, want err, Acknowledged=false, Settled=true, calls=0", result, err, calls.Load())
		}
	})

	t.Run("response_lost", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			hijackAndClose(t, w, false)
		}))
		defer server.Close()

		result, err := retryingClient(t, server.URL, 3, nil).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err == nil || result.Acknowledged || result.Settled || calls.Load() != 3 {
			t.Fatalf("got result=%+v err=%v calls=%d, want err, Acknowledged=false, Settled=false, calls=3", result, err, calls.Load())
		}
	})

	t.Run("deadline_after_send", func(t *testing.T) {
		received := make(chan struct{})
		var once sync.Once
		release := make(chan struct{})
		defer close(release)
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			once.Do(func() { close(received) })
			select {
			case <-r.Context().Done():
			case <-release:
			}
		}))
		defer server.Close()

		ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
		defer cancel()
		result, err := retryingClient(t, server.URL, 1, nil).PutSource(ctx, "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		select {
		case <-received:
		default:
			t.Fatal("request did not reach server before deadline")
		}
		if err == nil || result.Acknowledged || result.Settled {
			t.Fatalf("got result=%+v err=%v, want err, Acknowledged=false, Settled=false", result, err)
		}
	})

	t.Run("reset_during_body", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			_, _ = io.CopyN(io.Discard, r.Body, 64<<10)
			hijackAndClose(t, w, true)
		}))
		defer server.Close()

		body := bytes.NewReader(bytes.Repeat([]byte("b"), 3<<20))
		result, err := retryingClient(t, server.URL, 3, nil).PutSource(t.Context(), "documents/large.bin", extractor.FileTypeText, body)
		if err == nil || result.Acknowledged || result.Settled || calls.Load() != 3 {
			t.Fatalf("got result=%+v err=%v calls=%d, want err, Acknowledged=false, Settled=false, calls=3", result, err, calls.Load())
		}
	})

	t.Run("deadline_between_unsent_attempts", func(t *testing.T) {
		endpoint := "http://" + closedListenerAddr(t)
		var transportCalls atomic.Int32
		counting := httpClientFunc(func(req *http.Request) (*http.Response, error) {
			transportCalls.Add(1)
			return http.DefaultClient.Do(req)
		})
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		result, err := retryingClient(t, endpoint, 3, counting, 200*time.Millisecond).PutSource(ctx, "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err == nil || result.Acknowledged || !result.Settled || transportCalls.Load() != 1 {
			t.Fatalf("got result=%+v err=%v transportCalls=%d, want err, Acknowledged=false, Settled=true, transportCalls=1", result, err, transportCalls.Load())
		}
	})

	t.Run("transport_without_trace", func(t *testing.T) {
		custom := httpClientFunc(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("custom transport failure")
		})
		result, err := retryingClient(t, "http://127.0.0.1:1", 1, custom).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err == nil || result.Acknowledged || result.Settled {
			t.Fatalf("got result=%+v err=%v, want err, Acknowledged=false, Settled=false", result, err)
		}
	})

	t.Run("transport_response_with_error", func(t *testing.T) {
		custom := httpClientFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Header:     http.Header{"X-Amz-Request-Id": {"req-with-err"}},
				Body:       http.NoBody,
			}, errors.New("response returned with error")
		})
		result, err := retryingClient(t, "http://127.0.0.1:1", 1, custom).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
		if err == nil || result.Acknowledged || result.Settled {
			t.Fatalf("got result=%+v err=%v, want err, Acknowledged=false, Settled=false", result, err)
		}
	})

	t.Run("single_attempt", func(t *testing.T) {
		t.Run("refused", func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				writeS3Error(w, http.StatusForbidden, "AccessDenied", "req-403")
			}))
			defer server.Close()

			result, err := retryingClient(t, server.URL, 1, nil).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
			if err == nil || result.Acknowledged || !result.Settled {
				t.Fatalf("got result=%+v err=%v, want err, Acknowledged=false, Settled=true", result, err)
			}
		})

		t.Run("lost_response", func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				hijackAndClose(t, w, false)
			}))
			defer server.Close()

			result, err := retryingClient(t, server.URL, 1, nil).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
			if err == nil || result.Acknowledged || result.Settled {
				t.Fatalf("got result=%+v err=%v, want err, Acknowledged=false, Settled=false", result, err)
			}
		})
	})
}

func TestPutSourceAccountsForAttemptsOverHTTP2(t *testing.T) {
	for _, tc := range []struct {
		name        string
		abortStream bool
		wantSettled bool
	}{
		{name: "refused_403", abortStream: false, wantSettled: true},
		{name: "stream_aborted_after_body", abortStream: true, wantSettled: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sawHTTP2 atomic.Bool
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.ProtoMajor == 2 {
					sawHTTP2.Store(true)
				}
				_, _ = io.Copy(io.Discard, r.Body)
				if tc.abortStream {
					panic(http.ErrAbortHandler)
				}
				writeS3Error(w, http.StatusForbidden, "AccessDenied", "req-h2-403")
			}))
			server.EnableHTTP2 = true
			server.StartTLS()
			defer server.Close()

			result, err := retryingClient(t, server.URL, 2, server.Client()).PutSource(t.Context(), "documents/key.txt", extractor.FileTypeText, strings.NewReader("payload"))
			if err == nil || result.Acknowledged || result.Settled != tc.wantSettled || !sawHTTP2.Load() {
				t.Fatalf("got result=%+v err=%v sawHTTP2=%v, want err, Acknowledged=false, Settled=%v, HTTP/2", result, err, sawHTTP2.Load(), tc.wantSettled)
			}
		})
	}
}

func TestPutSourceAttemptsArePerCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if strings.HasPrefix(r.URL.Path, "/owned-bucket/documents/refuse/") {
			writeS3Error(w, http.StatusForbidden, "AccessDenied", "req-concurrent")
			return
		}
		hijackAndClose(t, w, false)
	}))
	defer server.Close()

	client := retryingClient(t, server.URL, 2, nil)
	const workers = 32
	type outcome struct {
		wantSettled bool
		result      domain.SourceUploadResult
		err         error
	}
	outcomes := make([]outcome, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			refuse := idx%2 == 0
			key := fmt.Sprintf("documents/lost/%d.txt", idx)
			if refuse {
				key = fmt.Sprintf("documents/refuse/%d.txt", idx)
			}
			res, err := client.PutSource(t.Context(), key, extractor.FileTypeText, strings.NewReader("payload"))
			outcomes[idx] = outcome{wantSettled: refuse, result: res, err: err}
		}(i)
	}
	wg.Wait()

	for i, o := range outcomes {
		if o.err == nil || o.result.Acknowledged || o.result.Settled != o.wantSettled {
			t.Fatalf("worker %d: result=%+v err=%v, want err, Acknowledged=false, Settled=%v", i, o.result, o.err, o.wantSettled)
		}
	}
}
