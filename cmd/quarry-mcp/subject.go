package main

import (
	"context"
	"encoding/json/jsontext"
	"strings"
	"time"

	"github.com/Victoroide/quarry-rag/client"
	"github.com/Victoroide/quarry-rag/internal/credential"
)

func subjectTokenFile(path string) client.SubjectCredentialProvider {
	return func(ctx context.Context) (client.SubjectCredential, error) {
		if err := ctx.Err(); err != nil {
			return client.SubjectCredential{}, err
		}
		// Allow one optional line terminator in addition to the encoded token.
		data, err := credential.ReadFile(path, credential.MaxTokenBytes+2, 0o077)
		if err != nil {
			return client.SubjectCredential{}, client.ErrSubjectCredential
		}
		token := string(data)
		if withoutNewline, ok := strings.CutSuffix(token, "\n"); ok {
			token = strings.TrimSuffix(withoutNewline, "\r")
		}
		_, payload, ok := credential.CompactJWT(token)
		var claims map[string]jsontext.Value
		if !ok || credential.DecodeObject(payload, &claims) != nil {
			return client.SubjectCredential{}, client.ErrSubjectCredential
		}
		// This is a local freshness check. Only the server authenticates the JWT.
		expiresAt, ok := credential.NumericDate(claims["exp"])
		if !ok || !time.Now().Before(expiresAt) {
			return client.SubjectCredential{}, client.ErrSubjectCredential
		}
		return client.SubjectCredential{Token: token, ExpiresAt: expiresAt}, nil
	}
}
