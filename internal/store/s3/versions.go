package s3

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// ErrVersionNotFound reports that an object version no longer exists.
var ErrVersionNotFound = errors.New("s3: object version not found")

// ErrVersionIDMissing reports that a version cannot be addressed by ID.
var ErrVersionIDMissing = errors.New("s3: object version ID missing")

// ErrVersionSizeInvalid reports that a data version has no usable size.
var ErrVersionSizeInvalid = errors.New("s3: object version size missing or negative")

// ObjectVersion is one stored version or delete marker of an exact object key.
// An unversioned bucket reports its single object with the version ID "null".
type ObjectVersion struct {
	ID           string
	Size         int64
	DeleteMarker bool
}

// ObjectLock describes retention and legal-hold protection of one version.
type ObjectLock struct {
	LegalHold   bool
	RetainUntil time.Time
}

// Protected reports whether the lock forbids deletion at the given time.
func (l ObjectLock) Protected(now time.Time) bool {
	return l.LegalHold || l.RetainUntil.After(now)
}

const maxVersionPages = 10

// ListVersions returns every version and delete marker of exactly key, newest
// first within each kind. It returns at most limit entries; complete is false
// when more exist or the listing could not be exhausted, so callers must not
// treat the result as all of the key's history.
func (c *Client) ListVersions(ctx context.Context, key string, limit int) (versions []ObjectVersion, complete bool, err error) {
	var keyMarker, versionMarker *string
	seenVersionMarkers := make(map[string]struct{})
	for range maxVersionPages {
		out, listErr := c.s3Client.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket:              &c.bucket,
			Prefix:              &key,
			KeyMarker:           keyMarker,
			VersionIdMarker:     versionMarker,
			ExpectedBucketOwner: c.owner,
		})
		if listErr != nil {
			return nil, false, fmt.Errorf("s3: list versions of %s: %w", key, listErr)
		}
		if out.IsTruncated == nil {
			return nil, false, nil
		}
		passed := false
		for _, v := range out.Versions {
			versionKey := aws.ToString(v.Key)
			switch {
			case versionKey == "" || !strings.HasPrefix(versionKey, key):
				return nil, false, nil
			case versionKey != key:
				passed = true
			case aws.ToString(v.VersionId) == "":
				return nil, false, fmt.Errorf("s3: list versions of %s: %w", key, ErrVersionIDMissing)
			case v.Size == nil || *v.Size < 0:
				return nil, false, fmt.Errorf("s3: list versions of %s: %w", key, ErrVersionSizeInvalid)
			case len(versions) >= limit:
				return versions, false, nil
			default:
				versions = append(versions, ObjectVersion{ID: aws.ToString(v.VersionId), Size: *v.Size})
			}
		}
		for _, m := range out.DeleteMarkers {
			markerKey := aws.ToString(m.Key)
			switch {
			case markerKey == "" || !strings.HasPrefix(markerKey, key):
				return nil, false, nil
			case markerKey != key:
				passed = true
			case aws.ToString(m.VersionId) == "":
				return nil, false, fmt.Errorf("s3: list delete markers of %s: %w", key, ErrVersionIDMissing)
			case len(versions) >= limit:
				return versions, false, nil
			default:
				versions = append(versions, ObjectVersion{ID: aws.ToString(m.VersionId), DeleteMarker: true})
			}
		}
		// Listings sort by key, so the exact key precedes any longer key sharing its prefix.
		if passed || !*out.IsTruncated {
			return versions, true, nil
		}
		// A key-only cursor can skip the rest of this key's versions.
		nextVersion := aws.ToString(out.NextVersionIdMarker)
		if aws.ToString(out.NextKeyMarker) != key || nextVersion == "" {
			return nil, false, nil
		}
		if _, seen := seenVersionMarkers[nextVersion]; seen {
			return nil, false, nil
		}
		seenVersionMarkers[nextVersion] = struct{}{}
		keyMarker, versionMarker = out.NextKeyMarker, out.NextVersionIdMarker
	}
	return versions, false, nil
}

// VersionLock reads the retention and legal-hold state of one object version.
func (c *Client) VersionLock(ctx context.Context, key, versionID string) (ObjectLock, error) {
	input := &s3.HeadObjectInput{Bucket: &c.bucket, Key: &key, ExpectedBucketOwner: c.owner}
	if versionID != "" {
		input.VersionId = &versionID
	}
	out, err := c.s3Client.HeadObject(ctx, input)
	if err != nil {
		var responseErr *awshttp.ResponseError
		if errors.As(err, &responseErr) && responseErr.HTTPStatusCode() == http.StatusNotFound {
			return ObjectLock{}, fmt.Errorf("%w: %s", ErrVersionNotFound, key)
		}
		return ObjectLock{}, fmt.Errorf("s3: read lock of %s: %w", key, err)
	}
	lock := ObjectLock{LegalHold: out.ObjectLockLegalHoldStatus == types.ObjectLockLegalHoldStatusOn}
	if out.ObjectLockRetainUntilDate != nil {
		lock.RetainUntil = *out.ObjectLockRetainUntilDate
	}
	return lock, nil
}
