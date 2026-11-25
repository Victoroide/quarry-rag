package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	pgstore "github.com/Victoroide/quarry-rag/internal/store/postgres"
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// pendingUploadJournal supplies an ordered batch and records its deferrals.
type pendingUploadJournal struct {
	uploadJournal
	due      []pgstore.SourceUploadRecord
	deferred map[string]string
}

func (j *pendingUploadJournal) ClaimSourceUploads(context.Context, pgstore.SourceUploadSelection, string, time.Duration) ([]pgstore.SourceUploadRecord, error) {
	due := j.due
	j.due = nil
	return due, nil
}

func (*pendingUploadJournal) ReferencedSourceKeys(context.Context, []string) (map[string]bool, error) {
	return nil, nil
}

func (*pendingUploadJournal) SourceUploadSummaries(context.Context, string) ([]pgstore.SourceUploadSummary, error) {
	return nil, nil
}

func (j *pendingUploadJournal) DeferSourceUpload(_ context.Context, id, _, reason string, _ time.Duration) (bool, error) {
	j.deferred[id] = reason
	return true, nil
}

func pendingUploadRow(bucket *bucketDouble) pgstore.SourceUploadRecord {
	id, doc := uuid.NewString(), uuid.NewString()
	return pgstore.SourceUploadRecord{
		ID: id, DocumentID: doc, Bucket: bucket.name, State: "claimed", Now: time.Now(),
		Key: bucket.NewKey(doc, id, "document", "note.txt"),
	}
}

type lostDeleteReplyBucket struct {
	*bucketDouble
	firstKey string
}

func (b *lostDeleteReplyBucket) DeleteVersion(ctx context.Context, key, version string) error {
	if err := b.bucketDouble.DeleteVersion(ctx, key, version); err != nil {
		return err
	}
	if key == b.firstKey {
		return context.DeadlineExceeded
	}
	return nil
}

func TestLostDeleteReplyReservesBytesAcrossUploads(t *testing.T) {
	bucket := newBucket("lost-delete-reply", true)
	first, second := pendingUploadRow(bucket), pendingUploadRow(bucket)
	bucket.addVersion(first.Key, make([]byte, 8))
	bucket.addVersion(second.Key, make([]byte, 8))
	journal := &pendingUploadJournal{due: []pgstore.SourceUploadRecord{first, second}, deferred: make(map[string]string)}
	opts := applyOptions()
	opts.MaxBytes = 8

	report, err := reconcileSourceUploads(t.Context(), journal, &lostDeleteReplyBucket{bucket, first.Key}, bucket.name, opts)
	require.NoError(t, err)
	require.Zero(t, bucket.versionCount(first.Key), "the first version was deleted before the reply was lost")
	require.Equal(t, 1, bucket.versionCount(second.Key), "the next key cannot spend the reserved bytes")
	require.Zero(t, report.DeletedVersions)
	require.Zero(t, report.DeletedBytes)
	require.Equal(t, 2, report.Pending)
	require.Equal(t, map[string]string{first.ID: reasonDeleteFailed, second.ID: reasonByteLimit}, journal.deferred)
}

func TestSourceUploadByteBudgetRequiresKnownVersionSizes(t *testing.T) {
	for _, tc := range []struct {
		name, sizeXML string
		headSize      int
		wantDeletes   int
		wantReason    string
	}{
		{"oversized", "<Size>84</Size>", 84, 0, reasonByteLimit},
		{"explicit zero", "<Size>0</Size>", 0, 2, reasonAwaitingSettlement},
		{"missing size", "", 84, 0, reasonVersionSizeInvalid},
		{"negative size", "<Size>-1</Size>", 84, 0, reasonVersionSizeInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bucket := newBucket("version-sizes", true)
			row := pendingUploadRow(bucket)
			var deletes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				version := r.URL.Query().Get("versionId")
				switch {
				case r.Method == http.MethodGet && r.URL.Query().Has("versions") && r.URL.Query().Get("prefix") == row.Key:
					w.Header().Set("Content-Type", "application/xml")
					_, _ = fmt.Fprintf(w, `<ListVersionsResult><IsTruncated>false</IsTruncated>
						<Version><Key>%s</Key><VersionId>v1</VersionId>%s</Version>
						<DeleteMarker><Key>%s</Key><VersionId>marker</VersionId></DeleteMarker>
						</ListVersionsResult>`, row.Key, tc.sizeXML, row.Key)
				case r.Method == http.MethodHead && version == "v1":
					w.Header().Set("Content-Length", strconv.Itoa(tc.headSize))
				case r.Method == http.MethodDelete && (version == "v1" || version == "marker"):
					deletes.Add(1)
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			t.Cleanup(server.Close)
			t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
			t.Setenv("AWS_ENDPOINT_URL_S3", server.URL)
			objects, err := s3store.New(t.Context(), &config.Config{
				S3Bucket: bucket.name, AWSRegion: "us-east-1",
				AWSAccessKeyID: "fixture", AWSSecretKey: "fixture", AWSMaxAttempts: 1,
			})
			require.NoError(t, err)
			journal := &pendingUploadJournal{due: []pgstore.SourceUploadRecord{row}, deferred: make(map[string]string)}
			opts := applyOptions()
			opts.MaxBytes = 1
			report, err := reconcileSourceUploads(t.Context(), journal, objects, bucket.name, opts)
			require.NoError(t, err)
			require.EqualValues(t, tc.wantDeletes, deletes.Load())
			require.Equal(t, tc.wantDeletes, report.DeletedVersions)
			require.Zero(t, report.DeletedBytes)
			require.Equal(t, tc.wantReason, journal.deferred[row.ID])
		})
	}
}

func TestIndeterminateVersionListingLeavesTheWholeKeyPending(t *testing.T) {
	const continuation = "<IsTruncated>true</IsTruncated><NextKeyMarker>%[1]s</NextKeyMarker><NextVersionIdMarker>v1</NextVersionIdMarker>"
	for _, tc := range []struct {
		name, pagination, entry string
		repeatCursor            bool
		wantPages, wantDeletes  int32
		wantReason              string
	}{
		{"missing completion", "<NextKeyMarker>%[1]s</NextKeyMarker><NextVersionIdMarker>v1</NextVersionIdMarker>", "", false, 0, 0, reasonVersionsTruncated},
		{"version without key", "<IsTruncated>false</IsTruncated>", "<Version><VersionId>held</VersionId><Size>5</Size></Version>", false, 0, 0, reasonVersionsTruncated},
		{"marker without key", "<IsTruncated>false</IsTruncated>", "<DeleteMarker><VersionId>marker</VersionId></DeleteMarker>", false, 0, 0, reasonVersionsTruncated},
		{"marker with empty key", "<IsTruncated>false</IsTruncated>", "<DeleteMarker><Key></Key><VersionId>marker</VersionId></DeleteMarker>", false, 0, 0, reasonVersionsTruncated},
		{"next page with hold", continuation, "", false, 1, 0, reasonLegalHold},
		{"missing version marker", "<IsTruncated>true</IsTruncated><NextKeyMarker>%[1]s</NextKeyMarker>", "", false, 0, 0, reasonVersionsTruncated},
		{"wrong key marker", "<IsTruncated>true</IsTruncated><NextKeyMarker>%[1]s.bak</NextKeyMarker><NextVersionIdMarker>v1</NextVersionIdMarker>", "", false, 0, 0, reasonVersionsTruncated},
		{"repeated cursor", continuation, "", true, 1, 0, reasonVersionsTruncated},
		{"foreign version key", continuation, "<Version><Key>another-prefix/old-file</Key><VersionId>other</VersionId><Size>5</Size></Version>", false, 0, 0, reasonVersionsTruncated},
		{"foreign marker key", continuation, "<DeleteMarker><Key>another-prefix/old-file</Key><VersionId>marker</VersionId></DeleteMarker>", false, 0, 0, reasonVersionsTruncated},
		{"longer neighboring key", "<IsTruncated>true</IsTruncated>", "<Version><Key>%[1]s.bak</Key><VersionId>neighbor</VersionId><Size>5</Size></Version>", false, 0, 1, reasonAwaitingSettlement},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bucket := newBucket("version-listing", true)
			row := pendingUploadRow(bucket)
			var secondPages, deletes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				query := r.URL.Query()
				version := query.Get("versionId")
				switch {
				case r.Method == http.MethodGet && query.Has("versions") && query.Get("prefix") == row.Key:
					w.Header().Set("Content-Type", "application/xml")
					if query.Get("key-marker") == row.Key && query.Get("version-id-marker") == "v1" {
						page := secondPages.Add(1)
						switch {
						case tc.repeatCursor && page == 1:
							_, _ = fmt.Fprintf(w, "<ListVersionsResult>"+continuation+
								"<DeleteMarker><Key>%[1]s</Key><VersionId>marker</VersionId></DeleteMarker></ListVersionsResult>", row.Key)
						case tc.repeatCursor:
							_, _ = fmt.Fprint(w, "<ListVersionsResult><IsTruncated>false</IsTruncated></ListVersionsResult>")
						default:
							_, _ = fmt.Fprintf(w, "<ListVersionsResult><IsTruncated>false</IsTruncated><Version><Key>%s</Key><VersionId>held</VersionId><Size>5</Size></Version></ListVersionsResult>", row.Key)
						}
						return
					}
					if query.Has("key-marker") {
						secondPages.Add(1)
						_, _ = fmt.Fprint(w, "<ListVersionsResult><IsTruncated>false</IsTruncated></ListVersionsResult>")
						return
					}
					_, _ = fmt.Fprintf(w, "<ListVersionsResult>"+tc.pagination+
						"<Version><Key>%[1]s</Key><VersionId>v1</VersionId><Size>5</Size></Version>"+
						tc.entry+"</ListVersionsResult>", row.Key)
				case r.Method == http.MethodHead && version == "held":
					w.Header().Set("x-amz-object-lock-legal-hold", "ON")
				case r.Method == http.MethodHead && version == "v1":
					w.Header().Set("Content-Length", "5")
				case r.Method == http.MethodDelete && (version == "v1" || version == "marker"):
					deletes.Add(1)
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			t.Cleanup(server.Close)
			t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
			t.Setenv("AWS_ENDPOINT_URL_S3", server.URL)
			objects, err := s3store.New(t.Context(), &config.Config{
				S3Bucket: bucket.name, AWSRegion: "us-east-1",
				AWSAccessKeyID: "fixture", AWSSecretKey: "fixture", AWSMaxAttempts: 1,
			})
			require.NoError(t, err)
			journal := &pendingUploadJournal{due: []pgstore.SourceUploadRecord{row}, deferred: make(map[string]string)}

			report, err := reconcileSourceUploads(t.Context(), journal, objects, bucket.name, applyOptions())
			require.NoError(t, err)
			require.Equal(t, tc.wantPages, secondPages.Load())
			require.Equal(t, tc.wantDeletes, deletes.Load())
			require.EqualValues(t, tc.wantDeletes, report.DeletedVersions)
			require.Zero(t, report.Settled)
			require.Equal(t, tc.wantReason, journal.deferred[row.ID])
		})
	}
}
