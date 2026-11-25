package credential

import (
	"bytes"
	"encoding/base64"
	"encoding/json/jsontext"
	json "encoding/json/v2"
	"errors"
	"io"
	"math/big"
	"strconv"
	"strings"
	"time"
)

// MaxTokenBytes bounds an encoded compact access token.
const MaxTokenBytes = 8 << 10
const maxJSONDepth = 16

var errJSON = errors.New("invalid credential JSON")

// CompactJWT checks the size and canonical compact encoding of a signed JWT.
// It returns unverified data. Callers must verify signatures before trusting claims.
func CompactJWT(raw string) (header, payload []byte, ok bool) {
	if len(raw) > MaxTokenBytes {
		return nil, nil, false
	}
	first, rest, found := strings.Cut(raw, ".")
	middle, last, second := strings.Cut(rest, ".")
	if !found || !second || first == "" || middle == "" || last == "" || strings.ContainsAny(raw, "\r\n") {
		return nil, nil, false
	}
	decode := base64.RawURLEncoding.Strict().DecodeString
	header, err := decode(first)
	if err != nil {
		return nil, nil, false
	}
	payload, err = decode(middle)
	if err != nil {
		return nil, nil, false
	}
	_, err = decode(last)
	return header, payload, err == nil
}

// DecodeObject enforces a single JSON object, unique names at every depth and
// a bounded nesting depth before decoding into the caller's exact field types.
func DecodeObject(raw []byte, output any) error {
	decoder := jsontext.NewDecoder(bytes.NewReader(raw))
	if decoder.PeekKind() != '{' {
		return errJSON
	}
	for {
		if _, err := decoder.ReadToken(); err != nil || decoder.StackDepth() > maxJSONDepth {
			return errJSON
		}
		if decoder.StackDepth() == 0 {
			break
		}
	}
	if _, err := decoder.ReadToken(); !errors.Is(err, io.EOF) {
		return errJSON
	}
	return json.Unmarshal(raw, output)
}

// NumericDate decodes Unix seconds exactly at nanosecond resolution. Bound the
// number and its exponent before using arbitrary-precision arithmetic.
func NumericDate(raw jsontext.Value) (time.Time, bool) {
	if raw.Kind() != '0' || len(raw) > 64 {
		return time.Time{}, false
	}
	value := string(raw)
	if i := strings.IndexAny(value, "eE"); i >= 0 {
		exponent, err := strconv.Atoi(value[i+1:])
		if err != nil || exponent < -64 || exponent > 64 {
			return time.Time{}, false
		}
	}
	seconds, ok := new(big.Rat).SetString(value)
	if !ok {
		return time.Time{}, false
	}
	nanos := seconds.Mul(seconds, big.NewRat(int64(time.Second), 1))
	if !nanos.IsInt() {
		return time.Time{}, false
	}
	var fraction big.Int
	whole, _ := new(big.Int).QuoRem(nanos.Num(), big.NewInt(int64(time.Second)), &fraction)
	if !whole.IsInt64() {
		return time.Time{}, false
	}
	sec := whole.Int64()
	if sec < time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).Unix() || sec >= time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).Unix() {
		return time.Time{}, false
	}
	return time.Unix(sec, fraction.Int64()).UTC(), true
}
