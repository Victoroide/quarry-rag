package extractor

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/ascii85"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/ledongthuc/pdf"
)

const maxPDFImageSide = 10000

// Verify raster samples before accepting image-only evidence. The existing
// PDF reader exposes decoded data for raw, Flate and ASCII85 streams.
func checkPDFImage(ctx context.Context, source *pdfSource, image, resources pdf.Value, budget *int64, depth int) error {
	if depth > 4 {
		return errors.New("maximum image mask nesting exceeded")
	}
	width, height := image.Key("Width").Int64(), image.Key("Height").Int64()
	if width < 1 || height < 1 || width > maxPDFImageSide || height > maxPDFImageSide {
		return errors.New("invalid or over-budget image dimensions")
	}
	length := image.Key("Length").Int64()
	if length < 1 || length > maxSourceBytes {
		return errors.New("invalid image stream length")
	}
	components, bits := pdfImageSamples(image, resources)
	if components == 0 || (bits != 1 && bits != 2 && bits != 4 && bits != 8 && bits != 16) {
		return errors.New("unsupported image sample layout")
	}
	dct, err := pdfImageDCT(image)
	if err != nil {
		return err
	}
	if dct {
		if bits != 8 || image.Key("ImageMask").Bool() || pdfImageIndexed(image, resources) {
			return errors.New("JPEG image sample layout cannot be verified")
		}
		if err := checkPDFJPEG(ctx, source, image, length, pdfJPEGFrame{width: width, height: height, components: components}); err != nil {
			return err
		}
	} else {
		row := (width*components*bits + 7) / 8
		expected := row * height
		if expected > *budget {
			return errors.New("maximum decoded stream size exceeded")
		}
		if err := checkPDFImageFilters(image, row); err != nil {
			return err
		}
		reader, err := openPDFImage(source, image, length)
		if err != nil {
			return err
		}
		count, err := io.Copy(io.Discard, io.LimitReader(contextReader{ctx: ctx, reader: reader}, expected+1))
		closeErr := reader.Close()
		if readErr := errors.Join(err, closeErr); readErr != nil {
			return fmt.Errorf("decode image samples: %w", readErr)
		}
		if count != expected {
			return errors.New("image stream size does not match its samples")
		}
		*budget -= count
	}
	for _, name := range []string{"Mask", "SMask"} {
		mask := image.Key(name)
		if mask.Kind() == pdf.Stream {
			if err := checkPDFImage(ctx, source, mask, resources, budget, depth+1); err != nil {
				return err
			}
		}
	}
	return nil
}

func pdfImageColorSpace(image, resources pdf.Value) pdf.Value {
	space := image.Key("ColorSpace")
	if space.Kind() == pdf.Name {
		if resolved := resources.Key("ColorSpace").Key(space.Name()); !resolved.IsNull() {
			return resolved
		}
	}
	return space
}

func pdfImageIndexed(image, resources pdf.Value) bool {
	space := pdfImageColorSpace(image, resources)
	return space.Kind() == pdf.Array && space.Index(0).Name() == "Indexed"
}

func pdfImageSamples(image, resources pdf.Value) (components, bits int64) {
	bits = image.Key("BitsPerComponent").Int64()
	if image.Key("ImageMask").Bool() {
		if bits == 0 {
			bits = 1
		}
		if bits != 1 {
			return 0, 0
		}
		return 1, bits
	}
	space := pdfImageColorSpace(image, resources)
	switch space.Name() {
	case "DeviceGray":
		return 1, bits
	case "DeviceRGB":
		return 3, bits
	case "DeviceCMYK":
		return 4, bits
	}
	if space.Kind() == pdf.Array && space.Len() == 2 && space.Index(0).Name() == "ICCBased" {
		profile := space.Index(1)
		if n := profile.Key("N"); profile.Kind() == pdf.Stream && n.Kind() == pdf.Integer && (n.Int64() == 1 || n.Int64() == 3 || n.Int64() == 4) {
			return n.Int64(), bits
		}
		return 0, bits
	}
	if space.Kind() == pdf.Array && space.Index(0).Name() == "Indexed" {
		return 1, bits
	}
	return 0, bits
}

func pdfImageDCT(image pdf.Value) (bool, error) {
	filter, params := image.Key("Filter"), image.Key("DecodeParms")
	switch filter.Kind() {
	case pdf.Name:
		if filter.Name() != "DCTDecode" {
			return false, nil
		}
		return true, checkPDFDCTDecodeParms(params)
	case pdf.Array:
		if filter.Len() != 1 || filter.Index(0).Name() != "DCTDecode" {
			return false, nil
		}
		if params.IsNull() {
			return true, nil
		}
		if params.Kind() != pdf.Array || params.Len() != 1 {
			return false, errors.New("invalid JPEG decode parameters")
		}
		return true, checkPDFDCTDecodeParms(params.Index(0))
	default:
		return false, nil
	}
}

func checkPDFDCTDecodeParms(params pdf.Value) error {
	if params.IsNull() {
		return nil
	}
	if params.Kind() != pdf.Dict {
		return errors.New("invalid JPEG decode parameters")
	}
	keys := params.Keys()
	if len(keys) == 0 {
		return nil
	}
	if len(keys) != 1 || keys[0] != "ColorTransform" {
		return errors.New("invalid JPEG decode parameters")
	}
	ct := params.Key("ColorTransform")
	if ct.Kind() != pdf.Integer || (ct.Int64() != 0 && ct.Int64() != 1) {
		return errors.New("invalid JPEG decode parameters")
	}
	return nil
}

func checkPDFImageFilters(image pdf.Value, row int64) error {
	filter, params := image.Key("Filter"), image.Key("DecodeParms")
	switch filter.Kind() {
	case pdf.Null:
		return nil
	case pdf.Name:
		return checkPDFImageFilter(filter.Name(), params, row)
	case pdf.Array:
		if filter.Len() < 1 || filter.Len() > 2 || !params.IsNull() && (params.Kind() != pdf.Array || params.Len() != filter.Len()) {
			return errors.New("unsupported image stream filter count")
		}
		for i := range filter.Len() {
			if err := checkPDFImageFilter(filter.Index(i).Name(), params.Index(i), row); err != nil {
				return err
			}
		}
		return nil
	default:
		return errors.New("invalid image stream filter")
	}
}

func checkPDFImageFilter(name string, params pdf.Value, row int64) error {
	switch name {
	case "FlateDecode":
		if !params.IsNull() && params.Kind() != pdf.Dict {
			return errors.New("invalid image decode parameters")
		}
		predictor := params.Key("Predictor")
		if predictor.IsNull() || predictor.Int64() == 1 || (predictor.Int64() == 12 && params.Key("Columns").Int64() == row) {
			return nil
		}
	case "ASCII85Decode":
		if params.IsNull() {
			return nil
		}
	}
	return errors.New("image stream filter cannot be verified")
}

func openPDFImage(source *pdfSource, image pdf.Value, length int64) (io.ReadCloser, error) {
	filter, params := image.Key("Filter"), image.Key("DecodeParms")
	var explicitDefault, ascii85First bool
	if filter.Kind() == pdf.Array {
		last := filter.Len() - 1
		for i := range filter.Len() {
			if filter.Index(i).Name() == "FlateDecode" && params.Index(i).Key("Predictor").Int64() == 1 {
				if i != last || i > 0 && filter.Index(0).Name() != "ASCII85Decode" {
					return nil, errors.New("unsupported explicit Flate filter sequence")
				}
				explicitDefault, ascii85First = true, i > 0
			}
		}
	} else {
		explicitDefault = filter.Name() == "FlateDecode" && params.Key("Predictor").Int64() == 1
	}
	if !explicitDefault {
		return image.Reader(), nil
	}
	reader, err := verifiedPDFStream(source, image, length)
	if err != nil {
		return nil, err
	}
	if ascii85First {
		encoded, readErr := io.ReadAll(reader)
		if readErr != nil {
			return nil, readErr
		}
		reader = ascii85.NewDecoder(bytes.NewReader(bytes.TrimSuffix(bytes.TrimSpace(encoded), []byte("~>"))))
	}
	return zlib.NewReader(reader)
}

// The pinned reader exposes a stream's location in its textual representation,
// but cannot decode an explicit Flate predictor of 1 or JPEG streams. Verify both
// physical delimiters before reading a stream through the shared source budget.
func pdfStreamOffset(source *pdfSource, stream pdf.Value, length int64) (int64, error) {
	location := stream.String()
	marker := strings.LastIndex(location, ">>@")
	if marker < 0 {
		return 0, errors.New("invalid PDF stream location")
	}
	offset, parseErr := strconv.ParseInt(location[marker+len(">>@"):], 10, 64)
	if parseErr != nil || offset < 8 || offset > source.reader.Size()-length {
		return 0, errors.New("invalid PDF stream location")
	}
	var start [8]byte
	if _, err := source.ReadAt(start[:], offset-int64(len(start))); err != nil {
		return 0, err
	}
	if !bytes.HasSuffix(start[:], []byte("stream\n")) && !bytes.HasSuffix(start[:], []byte("stream\r\n")) {
		return 0, errors.New("invalid PDF stream start")
	}
	var end [12]byte
	n, err := source.ReadAt(end[:], offset+length)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, err
	}
	fields := bytes.Fields(end[:n])
	if len(fields) == 0 || !bytes.Equal(fields[0], []byte("endstream")) {
		return 0, errors.New("invalid PDF stream end")
	}
	return offset, nil
}

func verifiedPDFStream(source *pdfSource, stream pdf.Value, length int64) (io.Reader, error) {
	if source.encrypted {
		return nil, errors.New("explicit Flate predictor cannot be verified in an encrypted PDF")
	}
	offset, err := pdfStreamOffset(source, stream, length)
	if err != nil {
		return nil, err
	}
	return io.NewSectionReader(source, offset, length), nil
}

func checkPDFJPEG(ctx context.Context, source *pdfSource, image pdf.Value, length int64, want pdfJPEGFrame) error {
	if source.encrypted {
		return errors.New("JPEG image cannot be verified in an encrypted PDF")
	}
	offset, err := pdfStreamOffset(source, image, length)
	if err != nil {
		return err
	}
	frame, cached := source.jpegFrames[offset]
	if !cached {
		frame, err = readPDFJPEGFrame(contextReader{ctx: ctx, reader: io.NewSectionReader(source, offset, length)})
		if err != nil {
			return fmt.Errorf("invalid JPEG image: %w", err)
		}
	}
	if frame != want {
		return errors.New("JPEG frame does not match its image dictionary")
	}
	if source.jpegFrames == nil {
		source.jpegFrames = make(map[int64]pdfJPEGFrame)
	}
	source.jpegFrames[offset] = frame
	return nil
}
