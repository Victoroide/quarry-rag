package s3

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/extractor"
	"github.com/aws/aws-sdk-go-v2/aws"
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
