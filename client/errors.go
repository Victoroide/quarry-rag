package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// ErrorResponse is the standard HTTP error envelope.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// ExtractionErrorResponse is the file extraction error envelope.
type ExtractionErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Code    string `json:"code"`
}

// APIError describes a non-success HTTP response and its retry guidance.
type APIError struct {
	StatusCode     int
	Code           string
	Message        string
	ExtractionCode string
	RequestID      string
	RetryAfter     time.Duration
}

// Error returns the HTTP status and service error message.
func (e *APIError) Error() string { return fmt.Sprintf("%d %s: %s", e.StatusCode, e.Code, e.Message) }

func decodeAPIError(resp *http.Response) *APIError {
	err := &APIError{StatusCode: resp.StatusCode, Code: "http_error", Message: http.StatusText(resp.StatusCode), RequestID: resp.Header.Get("X-Request-ID")}
	var envelope ExtractionErrorResponse
	if decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&envelope); decodeErr == nil {
		if envelope.Error != "" {
			err.Code = envelope.Error
		}
		if envelope.Message != "" {
			err.Message = envelope.Message
		}
		err.ExtractionCode = envelope.Code
	}
	retry := resp.Header.Get("Retry-After")
	if seconds, parseErr := strconv.ParseInt(retry, 10, 64); parseErr == nil {
		if seconds > 0 && seconds <= int64((1<<63-1)/time.Second) {
			err.RetryAfter = time.Duration(seconds) * time.Second
		}
	} else if date, parseErr := http.ParseTime(retry); parseErr == nil {
		if delay := time.Until(date); delay > 0 {
			err.RetryAfter = delay
		}
	}
	return err
}
