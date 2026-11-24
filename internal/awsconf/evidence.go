package awsconf

import (
	"context"
	"net/http"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// GuardEvidenceSends checks each SDK HTTP attempt with the original query's
// scope. Disable automatic body replay below this boundary on guarded sends.
func GuardEvidenceSends(ctx context.Context, client aws.HTTPClient) aws.HTTPClient {
	if !domain.HasEvidenceCheck(ctx) {
		return client
	}
	return smithyhttp.ClientDoFunc(func(request *http.Request) (*http.Response, error) {
		if err := domain.CheckEvidenceBeforeSend(request.Context()); err != nil {
			return nil, err
		}
		request.GetBody = nil
		return client.Do(request)
	})
}
