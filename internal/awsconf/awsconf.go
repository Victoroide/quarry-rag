// Package awsconf configures shared AWS credentials and retries.
package awsconf

import (
	"context"
	"errors"
	"time"

	"github.com/Victoroide/quarry-rag/internal/config"
	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// Load uses static credentials when configured and the default chain otherwise.
func Load(ctx context.Context, cfg *config.Config) (aws.Config, error) {
	options := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.AWSRegion),
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.AddWithErrorCodes(retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = cfg.AWSMaxAttempts
				o.MaxBackoff = 10 * time.Second
				o.RateLimiter = ratelimit.None
				o.Retryables = append([]retry.IsErrorRetryable{retry.IsErrorRetryableFunc(func(err error) aws.Ternary {
					if errors.Is(err, domain.ErrEvidenceCheck) {
						return aws.FalseTernary
					}
					return aws.UnknownTernary
				})}, o.Retryables...)
			}), "ModelTimeoutException", "ModelNotReadyException")
		}),
	}
	if cfg.AWSAccessKeyID != "" && cfg.AWSSecretKey != "" {
		options = append(options, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretKey, cfg.AWSSessionToken),
		))
	}
	return awsconfig.LoadDefaultConfig(ctx, options...)
}
