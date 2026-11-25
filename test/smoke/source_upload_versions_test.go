package smoke

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/extractor"
	s3store "github.com/Victoroide/quarry-rag/internal/store/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestSmoke_VersionedBucketSemantics checks, against a disposable versioned
// bucket, the behavior source upload reclamation depends on: writes report a
// version, a plain delete only adds a marker, exact-key listing exposes both,
// and deleting by version ID removes them. It also needs SMOKE_S3_VERSIONED=1
// because it writes and deletes objects under documents/smoke/ in S3_BUCKET.
func TestSmoke_VersionedBucketSemantics(t *testing.T) {
	skipUnlessSmoke(t)
	if os.Getenv("SMOKE_S3_VERSIONED") != "1" {
		t.Skip("set SMOKE_S3_VERSIONED=1 with S3_BUCKET naming a disposable bucket that has versioning enabled")
	}
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.FakeMode || cfg.S3Bucket == "" {
		t.Fatal("versioned bucket checks require GO_RAG_FAKE_MODE=false and S3_BUCKET")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	client, err := s3store.New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	key := client.NewKey(uuid.NewString(), uuid.NewString(), "smoke", "versions.txt")
	if err := checkVersionedBucket(ctx, client, key); err != nil {
		t.Fatal(err)
	}
}

func checkVersionedBucket(ctx context.Context, client *s3store.Client, key string) (err error) {
	knownVersions := make(map[string]struct{})
	remember := func(versions []s3store.ObjectVersion) {
		for _, version := range versions {
			knownVersions[version.ID] = struct{}{}
		}
	}
	defer func() {
		cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer stop()
		versions, complete, listErr := client.ListVersions(cleanupCtx, key, 100)
		switch {
		case listErr != nil:
			err = errors.Join(err, fmt.Errorf("list leftovers: %w", listErr))
		case !complete:
			err = errors.Join(err, errors.New("list leftovers: complete = false"))
		default:
			clear(knownVersions)
		}
		remember(versions)
		for id := range knownVersions {
			if deleteErr := client.DeleteVersion(cleanupCtx, key, id); deleteErr != nil {
				err = errors.Join(err, fmt.Errorf("remove leftover %s: %w", id, deleteErr))
			}
		}
	}()

	version, err := client.Put(ctx, key, extractor.FileTypeText, strings.NewReader("versioned smoke object"))
	if err != nil {
		return err
	}
	if version == "" {
		return errors.New("the bucket reported no version. Enable versioning for this check")
	}
	knownVersions[version] = struct{}{}
	if deleteErr := client.Delete(ctx, key); deleteErr != nil {
		return deleteErr
	}
	versions, complete, err := client.ListVersions(ctx, key, 100)
	remember(versions)
	if err != nil {
		return fmt.Errorf("list versions after adding delete marker: %w", err)
	}
	if !complete {
		return fmt.Errorf("versions after adding delete marker: %+v, complete = false", versions)
	}
	var data, markers int
	for _, v := range versions {
		if v.DeleteMarker {
			markers++
		} else if v.ID == version {
			data++
		}
	}
	if data != 1 || markers != 1 {
		return fmt.Errorf("a plain delete must keep the written version behind one marker, got %+v", versions)
	}
	if _, err = client.VersionLock(ctx, key, version); err != nil {
		return fmt.Errorf("read version lock: %w", err)
	}
	for _, v := range versions {
		if err = client.DeleteVersion(ctx, key, v.ID); err != nil {
			return fmt.Errorf("delete version %s: %w", v.ID, err)
		}
	}
	remaining, complete, err := client.ListVersions(ctx, key, 100)
	remember(remaining)
	if err != nil {
		return fmt.Errorf("list versions after deletion by version ID: %w", err)
	}
	if !complete || len(remaining) != 0 {
		return fmt.Errorf("versions after deletion by version ID: %+v, complete = %v", remaining, complete)
	}
	return nil
}

func TestFinalVersionObservation(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		finalEmpty, cleanupEmpty bool
	}{
		{name: "complete empty history", finalEmpty: true, cleanupEmpty: true},
		{name: "indeterminate final history", cleanupEmpty: true},
		{name: "indeterminate cleanup history", finalEmpty: true},
		{name: "indeterminate final and cleanup histories"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := "documents/smoke/" + uuid.NewString() + "/versions.txt"
			var listings, dataDeletes, markerDeletes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				query := r.URL.Query()
				switch {
				case r.Method == http.MethodPut:
					w.Header().Set("x-amz-version-id", "saved")
				case r.Method == http.MethodDelete:
					switch query.Get("versionId") {
					case "saved":
						dataDeletes.Add(1)
					case "marker":
						markerDeletes.Add(1)
					case "":
					default:
						t.Errorf("unexpected version ID: %s", query.Get("versionId"))
					}
					w.WriteHeader(http.StatusNoContent)
				case r.Method == http.MethodGet && query.Has("versions") && query.Get("prefix") == key:
					w.Header().Set("Content-Type", "application/xml")
					page := listings.Add(1)
					switch {
					case page == 1:
						_, _ = fmt.Fprintf(w, `<ListVersionsResult><IsTruncated>false</IsTruncated>
							<Version><Key>%[1]s</Key><VersionId>saved</VersionId><Size>22</Size></Version>
							<DeleteMarker><Key>%[1]s</Key><VersionId>marker</VersionId></DeleteMarker>
							</ListVersionsResult>`, key)
					case page == 2 && tc.finalEmpty, page == 3 && tc.cleanupEmpty:
						_, _ = fmt.Fprint(w, "<ListVersionsResult><IsTruncated>false</IsTruncated></ListVersionsResult>")
					default:
						_, _ = fmt.Fprint(w, "<ListVersionsResult/>")
					}
				case r.Method == http.MethodHead && query.Get("versionId") == "saved":
					w.Header().Set("Content-Length", "22")
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			t.Cleanup(server.Close)
			t.Setenv("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "false")
			t.Setenv("AWS_ENDPOINT_URL_S3", server.URL)
			client, err := s3store.New(t.Context(), &config.Config{
				S3Bucket: "versioned-smoke-fixture", AWSRegion: "us-east-1",
				AWSAccessKeyID: "fixture", AWSSecretKey: "fixture", AWSMaxAttempts: 1,
			})
			require.NoError(t, err)
			err = checkVersionedBucket(t.Context(), client, key)

			require.EqualValues(t, 3, listings.Load())
			if !tc.finalEmpty {
				require.ErrorContains(t, err, "versions after deletion by version ID: [], complete = false")
			}
			if !tc.cleanupEmpty {
				require.ErrorContains(t, err, "list leftovers: complete = false")
				require.EqualValues(t, 2, dataDeletes.Load())
				require.EqualValues(t, 2, markerDeletes.Load())
			} else {
				require.EqualValues(t, 1, dataDeletes.Load())
				require.EqualValues(t, 1, markerDeletes.Load())
			}
			if tc.finalEmpty && tc.cleanupEmpty {
				require.NoError(t, err)
			}
		})
	}
}
