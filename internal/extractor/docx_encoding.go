package extractor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/xml"
	"errors"
	"io"
	"strings"
	"unicode/utf16"
	"unicode/utf8"

	"golang.org/x/text/transform"
)

type docxEncoding string

const (
	docxUTF8    docxEncoding = "UTF-8"
	docxUTF16LE docxEncoding = "UTF-16LE"
	docxUTF16BE docxEncoding = "UTF-16BE"
)

func (encoding docxEncoding) matches(label string) bool {
	return strings.EqualFold(string(encoding), label) || (encoding != docxUTF8 && strings.EqualFold("UTF-16", label))
}

func docxXMLSource(ctx context.Context, r io.Reader) (io.Reader, docxEncoding) {
	source := bufio.NewReader(contextReader{ctx: ctx, reader: r})
	prefix, _ := source.Peek(3)
	switch {
	case bytes.HasPrefix(prefix, []byte("\xff\xfe")):
		_, _ = source.Discard(2)
		return transform.NewReader(source, docxUTF16Decoder{order: binary.LittleEndian}), docxUTF16LE
	case bytes.HasPrefix(prefix, []byte("\xfe\xff")):
		_, _ = source.Discard(2)
		return transform.NewReader(source, docxUTF16Decoder{order: binary.BigEndian}), docxUTF16BE
	case bytes.Equal(prefix, []byte("\xef\xbb\xbf")):
		_, _ = source.Discard(3)
	}
	return source, docxUTF8
}

type docxUTF16Decoder struct {
	transform.NopResetter
	order binary.ByteOrder
}

func (d docxUTF16Decoder) Transform(dst, src []byte, atEOF bool) (nDst, nSrc int, err error) {
	for nSrc < len(src) {
		remaining := src[nSrc:]
		if len(remaining) < 2 {
			break
		}
		r, width := rune(d.order.Uint16(remaining)), 2
		if utf16.IsSurrogate(r) {
			if r >= 0xdc00 {
				return nDst, nSrc, errors.New("unpaired UTF-16 surrogate")
			}
			if len(remaining) < 4 {
				break
			}
			r, width = utf16.DecodeRune(r, rune(d.order.Uint16(remaining[2:]))), 4
			if r == utf8.RuneError {
				return nDst, nSrc, errors.New("unpaired UTF-16 surrogate")
			}
		}
		if len(dst)-nDst < utf8.RuneLen(r) {
			return nDst, nSrc, transform.ErrShortDst
		}
		nDst += utf8.EncodeRune(dst[nDst:], r)
		nSrc += width
	}
	if nSrc == len(src) {
		return nDst, nSrc, nil
	}
	if atEOF {
		return nDst, nSrc, io.ErrUnexpectedEOF
	}
	return nDst, nSrc, transform.ErrShortSrc
}

func validateDOCXXMLDeclaration(instruction []byte, encoding docxEncoding) error {
	const prefix, suffix = "<xml ", "/>"
	source := io.MultiReader(strings.NewReader(prefix), bytes.NewReader(instruction), strings.NewReader(suffix))
	decoder := xml.NewDecoder(source)
	token, err := decoder.RawToken()
	start, ok := token.(xml.StartElement)
	if err != nil || !ok || decoder.InputOffset() != int64(len(prefix)+len(instruction)+len(suffix)) {
		return errors.New("invalid XML declaration")
	}
	attrs := start.Attr
	if len(attrs) == 0 || attrs[0].Name != (xml.Name{Local: "version"}) || attrs[0].Value != "1.0" {
		return errors.New("invalid XML version declaration")
	}
	attrs = attrs[1:]
	if len(attrs) > 0 && attrs[0].Name == (xml.Name{Local: "encoding"}) {
		if !encoding.matches(attrs[0].Value) {
			return errors.New("XML encoding declaration disagrees with part encoding")
		}
		attrs = attrs[1:]
	}
	if len(attrs) == 1 && attrs[0].Name == (xml.Name{Local: "standalone"}) && (attrs[0].Value == "yes" || attrs[0].Value == "no") {
		attrs = attrs[1:]
	}
	if len(attrs) != 0 {
		return errors.New("invalid XML declaration")
	}
	return nil
}
