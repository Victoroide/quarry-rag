package textutil

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// NormalizeLabel canonicalizes a service label and validates its character and byte limits.
func NormalizeLabel(value string) (string, error) {
	if !utf8.ValidString(value) {
		return "", errors.New("must be valid UTF-8")
	}
	value = norm.NFC.String(strings.TrimSpace(value))
	if len(value) > 128 {
		return "", errors.New("exceeds maximum length of 128 bytes")
	}
	for _, r := range value {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return "", errors.New("must not contain control or format characters")
		}
	}
	return value, nil
}
