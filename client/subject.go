package client

import (
	"context"
	"errors"
	"strings"
	"time"
)

// ErrSubjectCredential means no current subject credential could be obtained.
var ErrSubjectCredential = errors.New("subject credential unavailable or expired")

// SubjectCredential is a delegated JWT access token and its exact expiry.
type SubjectCredential struct {
	Token     string
	ExpiresAt time.Time
}

// SubjectCredentialProvider supplies a current subject credential for each HTTP
// call using the context after request options have been applied. The provider
// must support concurrent calls when shared by callers.
type SubjectCredentialProvider func(context.Context) (SubjectCredential, error)

// WithSubjectCredentialProvider adds a bearer credential alongside the service
// token. A provider failure prevents the request from being sent.
func WithSubjectCredentialProvider(provider SubjectCredentialProvider) Option {
	return func(o *options) error {
		if provider == nil {
			return errors.New("subject credential provider is required")
		}
		o.subject = provider
		return nil
	}
}

type requestCredentials struct {
	service string
	subject SubjectCredential
}

func (c requestCredentials) redact(value string) string {
	const replacement = "[redacted]"
	if c.subject.Token != "" && c.service != "" {
		return strings.NewReplacer(c.subject.Token, replacement, c.service, replacement).Replace(value)
	}
	if c.service != "" {
		return strings.ReplaceAll(value, c.service, replacement)
	}
	return value
}

func (c requestCredentials) safeError(err error) error {
	message := err.Error()
	if safe := c.redact(message); safe != message {
		return errors.New(safe)
	}
	return err
}
