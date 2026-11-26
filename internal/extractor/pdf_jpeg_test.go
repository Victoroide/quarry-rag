package extractor

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"testing"

	"github.com/Victoroide/quarry-rag/internal/testutil"
	"github.com/ledongthuc/pdf"
	"github.com/stretchr/testify/require"
)

func encodeTestJPEG(t testing.TB, img image.Image) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, jpeg.Encode(&buf, img, nil))
	return buf.Bytes()
}

func makeJPEGStream(parts ...[]byte) []byte {
	var buf bytes.Buffer
	for _, part := range parts {
		buf.Write(part)
	}
	return buf.Bytes()
}

func jpegMarkerSegment(code byte, payload []byte) []byte {
	n := len(payload) + 2
	out := make([]byte, 4+len(payload))
	out[0], out[1] = 0xFF, code
	out[2], out[3] = byte((n>>8)&0xff), byte(n&0xff)
	copy(out[4:], payload)
	return out
}

func dqtSegment(tq, pq byte) []byte {
	size := 64 * (int(pq) + 1)
	payload := make([]byte, 1+size)
	payload[0] = (pq << 4) | tq
	for i := 1; i < len(payload); i++ {
		payload[i] = 0x10
	}
	return jpegMarkerSegment(0xDB, payload)
}

func dhtPayload(tc, th byte, counts [16]byte) []byte {
	total := 0
	for _, c := range counts {
		total += int(c)
	}
	payload := make([]byte, 17+total)
	payload[0] = (tc << 4) | th
	copy(payload[1:17], counts[:])
	for i := range total {
		payload[17+i] = byte(i)
	}
	return payload
}

func dhtSegment(tc, th byte, counts [16]byte) []byte {
	return jpegMarkerSegment(0xC4, dhtPayload(tc, th, counts))
}

func minimalDHTCounts() [16]byte {
	var counts [16]byte
	counts[0] = 1
	return counts
}

func sofPayload(precision byte, height, width uint16, comps [][3]byte) []byte {
	payload := make([]byte, 6+3*len(comps))
	payload[0] = precision
	payload[1], payload[2] = byte((height>>8)&0xff), byte(height&0xff)
	payload[3], payload[4] = byte((width>>8)&0xff), byte(width&0xff)
	payload[5] = byte(len(comps) & 0xff)
	for i, c := range comps {
		payload[6+3*i] = c[0]
		payload[7+3*i] = c[1]
		payload[8+3*i] = c[2]
	}
	return payload
}

func sosPayload(comps [][2]byte, ss, se, ahAl byte) []byte {
	payload := make([]byte, 4+2*len(comps))
	payload[0] = byte(len(comps) & 0xff)
	for i, c := range comps {
		payload[1+2*i] = c[0]
		payload[2+2*i] = c[1]
	}
	payload[1+2*len(comps)] = ss
	payload[2+2*len(comps)] = se
	payload[3+2*len(comps)] = ahAl
	return payload
}

func TestReadPDFJPEGFrame(t *testing.T) {
	gray := encodeTestJPEG(t, image.NewGray(image.Rect(0, 0, 17, 29)))
	colorImg := image.NewRGBA(image.Rect(0, 0, 23, 31))
	colorImg.Set(0, 0, color.RGBA{R: 120, G: 45, B: 200, A: 255})
	rgb := encodeTestJPEG(t, colorImg)

	cmykSOF := jpegMarkerSegment(0xC0, sofPayload(8, 12, 16, [][3]byte{
		{1, 0x11, 0}, {2, 0x11, 1}, {3, 0x11, 1}, {4, 0x11, 0},
	}))
	cmykSOS := jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}, {2, 0x11}, {3, 0x11}, {4, 0x00}}, 0, 63, 0))
	cmykJPEG := makeJPEGStream(
		[]byte{0xFF, 0xD8},
		dqtSegment(0, 0), dqtSegment(1, 0),
		cmykSOF,
		dhtSegment(0, 0, minimalDHTCounts()), dhtSegment(1, 0, minimalDHTCounts()),
		dhtSegment(0, 1, minimalDHTCounts()), dhtSegment(1, 1, minimalDHTCounts()),
		cmykSOS,
		[]byte{0x01, 0x02, 0x03},
		[]byte{0xFF, 0xD9},
	)

	progJPEG := makeJPEGStream(
		[]byte{0xFF, 0xD8},
		dqtSegment(0, 0),
		jpegMarkerSegment(0xC2, sofPayload(8, 32, 48, [][3]byte{{1, 0x11, 0}})),
		dhtSegment(0, 0, minimalDHTCounts()),
		jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}}, 0, 0, 0x01)), // first DC scan
		[]byte{0x12, 0x34},
		jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}}, 0, 0, 0x10)), // DC refine (no DHT needed)
		[]byte{0x56, 0x78},
		dhtSegment(1, 2, minimalDHTCounts()),
		jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x02}}, 1, 63, 0x00)), // AC scan
		[]byte{0x9A, 0xBC},
		[]byte{0xFF, 0xD9},
	)

	validBaseline1x1 := makeJPEGStream(
		[]byte{0xFF, 0xD8},
		dqtSegment(0, 0),
		jpegMarkerSegment(0xC0, sofPayload(8, 1, 1, [][3]byte{{1, 0x11, 0}})),
		dhtSegment(0, 0, minimalDHTCounts()), dhtSegment(1, 0, minimalDHTCounts()),
		jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}}, 0, 63, 0)),
		[]byte{0x00},
		[]byte{0xFF, 0xD9},
	)

	valid10000 := makeJPEGStream(
		[]byte{0xFF, 0xD8},
		dqtSegment(0, 0),
		jpegMarkerSegment(0xC0, sofPayload(8, 10000, 10000, [][3]byte{{1, 0x11, 0}})),
		dhtSegment(0, 0, minimalDHTCounts()), dhtSegment(1, 0, minimalDHTCounts()),
		jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}}, 0, 63, 0)),
		[]byte{0x00},
		[]byte{0xFF, 0xD9},
	)

	validExtendedSOF1 := makeJPEGStream(
		[]byte{0xFF, 0xD8},
		dqtSegment(2, 1),
		jpegMarkerSegment(0xC1, sofPayload(8, 19, 27, [][3]byte{{1, 0x11, 2}})),
		dhtSegment(0, 3, minimalDHTCounts()), dhtSegment(1, 3, minimalDHTCounts()),
		jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x33}}, 0, 63, 0)),
		[]byte{0x42},
		[]byte{0xFF, 0xD9},
	)

	embeddedThumb := makeJPEGStream([]byte{0xFF, 0xD8, 0xFF, 0xDB, 0x00, 0x04, 0x01, 0x02, 0xFF, 0xD9})
	validTricky := makeJPEGStream(
		[]byte{0xFF, 0xD8},
		[]byte{0xFF, 0xFF, 0xFF}, // fill bytes
		jpegMarkerSegment(0xE1, embeddedThumb),
		[]byte{0xFF, 0x01, 0xFF, 0xD0}, // TEM and RST0 between segments
		dqtSegment(0, 0),
		jpegMarkerSegment(0xC0, sofPayload(8, 40, 50, [][3]byte{{1, 0x11, 0}})),
		jpegMarkerSegment(0xDC, []byte{0x00, 0x28}), // DNL with height > 0 already
		jpegMarkerSegment(0xDD, []byte{0x00, 0x01}), // DRI
		dhtSegment(0, 0, minimalDHTCounts()), dhtSegment(1, 0, minimalDHTCounts()),
		jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}}, 0, 63, 0)),
		[]byte{0x11, 0xFF, 0x00, 0x22, 0xFF, 0xD2, 0x33}, // FF 00 stuffing and RST2 inside entropy data
		[]byte{0xFF, 0xFF, 0xD9},                         // fill bytes before EOI
		[]byte{0xDE, 0xAD, 0xBE, 0xEF},                   // trailing bytes after EOI
	)

	t.Run("valid", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			data []byte
			want pdfJPEGFrame
		}{
			{"go gray baseline", gray, pdfJPEGFrame{width: 17, height: 29, components: 1}},
			{"go color baseline", rgb, pdfJPEGFrame{width: 23, height: 31, components: 3}},
			{"1x1", validBaseline1x1, pdfJPEGFrame{width: 1, height: 1, components: 1}},
			{"10000x10000", valid10000, pdfJPEGFrame{width: 10000, height: 10000, components: 1}},
			{"extended sof1 16bit dqt", validExtendedSOF1, pdfJPEGFrame{width: 27, height: 19, components: 1}},
			{"4-component baseline", cmykJPEG, pdfJPEGFrame{width: 16, height: 12, components: 4}},
			{"progressive multi-scan", progJPEG, pdfJPEGFrame{width: 48, height: 32, components: 1}},
			{"fill bytes stuffing rst tem dnl app1 thumbnail trailing", validTricky, pdfJPEGFrame{width: 50, height: 40, components: 1}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := readPDFJPEGFrame(bytes.NewReader(tc.data))
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
		}
	})

	t.Run("invalid", func(t *testing.T) {
		var zeroCounts, overSubCounts, sum257Counts [16]byte
		overSubCounts[0] = 3 // 3 codes of length 1 > 2^1
		sum257Counts[8] = 255
		sum257Counts[9] = 2 // code@9=255<=512, code@10=512<=1024, sum=257

		for _, tc := range []struct {
			name    string
			data    []byte
			wantErr string
		}{
			{"empty", nil, "truncated JPEG data"},
			{"missing SOI", []byte{0x00, 0xD8, 0xFF, 0xD9}, "missing start of image"},
			{"second SOI", makeJPEGStream([]byte{0xFF, 0xD8, 0xFF, 0xD8, 0xFF, 0xD9}), "unexpected start of image"},
			{"SOS before SOF", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0}}, 0, 63, 0)), []byte{0xFF, 0xD9}), "scan before frame header"},
			{"two SOF", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xC0, sofPayload(8, 1, 1, [][3]byte{{1, 0x11, 0}})), jpegMarkerSegment(0xC0, sofPayload(8, 1, 1, [][3]byte{{1, 0x11, 0}})), []byte{0xFF, 0xD9}), "multiple frame headers"},
			{"no SOS before EOI", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xC0, sofPayload(8, 1, 1, [][3]byte{{1, 0x11, 0}})), []byte{0xFF, 0xD9}), "missing JPEG scan"},
			{"truncated in entropy data", validBaseline1x1[:len(validBaseline1x1)-2], "truncated JPEG data"},
			{"truncated in segment header", []byte{0xFF, 0xD8, 0xFF, 0xC0, 0x00}, "truncated JPEG data"},
			{"missing EOI", validBaseline1x1[:len(validBaseline1x1)-1], "truncated JPEG data"},
			{"segment length below 2", []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x01, 0xFF, 0xD9}, "invalid segment length"},
			{"segment length past end", []byte{0xFF, 0xD8, 0xFF, 0xE0, 0x01, 0x00, 0x00, 0x00}, "truncated JPEG data"},
			{"non-FF marker byte", []byte{0xFF, 0xD8, 0x12, 0x34}, "invalid marker"},
			{"SOF3 lossless", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xC3, sofPayload(8, 1, 1, [][3]byte{{1, 0x11, 0}}))), "unsupported JPEG coding process"},
			{"SOF5 differential sequential", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xC5, sofPayload(8, 1, 1, [][3]byte{{1, 0x11, 0}}))), "unsupported JPEG coding process"},
			{"SOF9 arithmetic sequential", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xC9, sofPayload(8, 1, 1, [][3]byte{{1, 0x11, 0}}))), "unsupported JPEG coding process"},
			{"SOF10 arithmetic progressive", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xCA, sofPayload(8, 1, 1, [][3]byte{{1, 0x11, 0}}))), "unsupported JPEG coding process"},
			{"DAC", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xCC, []byte{0x00, 0x00})), "unsupported JPEG coding process"},
			{"DHP", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xDE, sofPayload(8, 1, 1, [][3]byte{{1, 0x11, 0}}))), "unsupported JPEG coding process"},
			{"EXP", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xDF, []byte{0x11})), "unsupported JPEG coding process"},
			{"JPGn", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xF0, []byte{0x00})), "unsupported JPEG marker"},
			{"reserved C8", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xC8, []byte{0x00})), "unsupported JPEG marker"},
			{"reserved 02", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0x02, []byte{0x00})), "unsupported JPEG marker"},
			{"12-bit precision", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xC0, sofPayload(12, 1, 1, [][3]byte{{1, 0x11, 0}}))), "unsupported JPEG sample precision"},
			{"height 0", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xC0, sofPayload(8, 0, 1, [][3]byte{{1, 0x11, 0}}))), "JPEG height defined later is unsupported"},
			{"width 0", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xC0, sofPayload(8, 1, 0, [][3]byte{{1, 0x11, 0}}))), "invalid JPEG width"},
			{"Nf 2", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xC0, sofPayload(8, 1, 1, [][3]byte{{1, 0x11, 0}, {2, 0x11, 0}}))), "unsupported JPEG component count"},
			{"duplicate component id", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xC0, sofPayload(8, 1, 1, [][3]byte{{1, 0x11, 0}, {1, 0x11, 0}, {2, 0x11, 0}}))), "duplicate JPEG component identifier"},
			{"H=5", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xC0, sofPayload(8, 1, 1, [][3]byte{{1, 0x51, 0}}))), "invalid JPEG sampling factor"},
			{"Tq=4", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xC0, sofPayload(8, 1, 1, [][3]byte{{1, 0x11, 4}}))), "invalid JPEG quantization table selector"},
			{"undefined Huffman table", makeJPEGStream([]byte{0xFF, 0xD8}, dqtSegment(0, 0), jpegMarkerSegment(0xC0, sofPayload(8, 1, 1, [][3]byte{{1, 0x11, 0}})), dhtSegment(0, 0, minimalDHTCounts()), jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}}, 0, 63, 0))), "undefined Huffman table"},
			{"undefined quantization table", makeJPEGStream([]byte{0xFF, 0xD8}, jpegMarkerSegment(0xC0, sofPayload(8, 1, 1, [][3]byte{{1, 0x11, 0}})), dhtSegment(0, 0, minimalDHTCounts()), dhtSegment(1, 0, minimalDHTCounts()), jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}}, 0, 63, 0))), "undefined quantization table"},
			{"over-subscribed DHT", makeJPEGStream([]byte{0xFF, 0xD8}, dhtSegment(0, 0, overSubCounts)), "invalid Huffman table"},
			{"Pq=2", makeJPEGStream([]byte{0xFF, 0xD8}, dqtSegment(0, 2)), "invalid quantization table"},
			{"DHT count sum 0", makeJPEGStream([]byte{0xFF, 0xD8}, dhtSegment(0, 0, zeroCounts)), "invalid Huffman table"},
			{"DHT count sum 257", makeJPEGStream([]byte{0xFF, 0xD8}, dhtSegment(0, 0, sum257Counts)), "invalid Huffman table"},
			{"progressive Ss>Se", makeJPEGStream([]byte{0xFF, 0xD8}, dqtSegment(0, 0), jpegMarkerSegment(0xC2, sofPayload(8, 8, 8, [][3]byte{{1, 0x11, 0}})), dhtSegment(1, 0, minimalDHTCounts()), jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}}, 5, 3, 0))), "invalid progressive scan parameters"},
			{"progressive AC scan Ns=2", makeJPEGStream([]byte{0xFF, 0xD8}, dqtSegment(0, 0), jpegMarkerSegment(0xC2, sofPayload(8, 8, 8, [][3]byte{{1, 0x11, 0}, {2, 0x11, 0}, {3, 0x11, 0}})), dhtSegment(1, 0, minimalDHTCounts()), jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}, {2, 0x00}}, 1, 63, 0))), "invalid progressive scan parameters"},
			{"sequential Se!=63", makeJPEGStream([]byte{0xFF, 0xD8}, dqtSegment(0, 0), jpegMarkerSegment(0xC0, sofPayload(8, 8, 8, [][3]byte{{1, 0x11, 0}})), dhtSegment(0, 0, minimalDHTCounts()), dhtSegment(1, 0, minimalDHTCounts()), jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}}, 0, 62, 0))), "invalid sequential scan parameters"},
			{"interleaved sum H*V=11", makeJPEGStream([]byte{0xFF, 0xD8}, dqtSegment(0, 0), jpegMarkerSegment(0xC0, sofPayload(8, 8, 8, [][3]byte{{1, 0x42, 0}, {2, 0x21, 0}, {3, 0x11, 0}})), dhtSegment(0, 0, minimalDHTCounts()), dhtSegment(1, 0, minimalDHTCounts()), jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}, {2, 0x00}, {3, 0x00}}, 0, 63, 0))), "invalid scan sampling factors"},
			{"scan component out of frame order", makeJPEGStream([]byte{0xFF, 0xD8}, dqtSegment(0, 0), jpegMarkerSegment(0xC0, sofPayload(8, 8, 8, [][3]byte{{1, 0x11, 0}, {2, 0x11, 0}, {3, 0x11, 0}})), dhtSegment(0, 0, minimalDHTCounts()), dhtSegment(1, 0, minimalDHTCounts()), jpegMarkerSegment(0xDA, sosPayload([][2]byte{{2, 0x00}, {1, 0x00}}, 0, 63, 0))), "invalid scan component order"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := readPDFJPEGFrame(bytes.NewReader(tc.data))
				require.ErrorContains(t, err, tc.wantErr)
				require.NotErrorIs(t, err, io.ErrUnexpectedEOF)
			})
		}
	})
}

func FuzzPDFJPEGFrame(f *testing.F) {
	f.Add(encodeTestJPEG(f, image.NewGray(image.Rect(0, 0, 8, 8))))
	f.Add(encodeTestJPEG(f, image.NewRGBA(image.Rect(0, 0, 12, 10))))
	f.Add(makeJPEGStream(
		[]byte{0xFF, 0xD8},
		dqtSegment(0, 0),
		jpegMarkerSegment(0xC2, sofPayload(8, 16, 16, [][3]byte{{1, 0x11, 0}})),
		dhtSegment(0, 0, minimalDHTCounts()),
		jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}}, 0, 0, 0x01)),
		[]byte{0x12},
		jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}}, 0, 0, 0x10)),
		[]byte{0x34},
		dhtSegment(1, 0, minimalDHTCounts()),
		jpegMarkerSegment(0xDA, sosPayload([][2]byte{{1, 0x00}}, 1, 63, 0)),
		[]byte{0x56},
		[]byte{0xFF, 0xD9},
	))
	f.Add([]byte{0xFF, 0xD8, 0xFF, 0xD9})
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 64<<10 {
			t.Skip()
		}
		frame, err := readPDFJPEGFrame(bytes.NewReader(data))
		if err != nil {
			return
		}
		require.Positive(t, frame.width)
		require.Positive(t, frame.height)
		require.Contains(t, []int64{1, 3, 4}, frame.components)

		if cfg, cfgErr := jpeg.DecodeConfig(bytes.NewReader(data)); cfgErr == nil {
			require.Equal(t, int64(cfg.Width), frame.width)
			require.Equal(t, int64(cfg.Height), frame.height)
			var wantComp int64
			switch cfg.ColorModel {
			case color.GrayModel:
				wantComp = 1
			case color.YCbCrModel, color.RGBAModel:
				wantComp = 3
			case color.CMYKModel:
				wantComp = 4
			}
			require.Equal(t, wantComp, frame.components)
		}
	})
}

type testJPEGFixture struct {
	source *pdfSource
	imgVal pdf.Value
	length int64
}

func pdfWithSingleJPEG(t *testing.T, jpegData []byte, w, h int, cs string) testJPEGFixture {
	t.Helper()
	raster := make([]byte, 0, len(jpegData)+160)
	raster = append(raster, []byte("<< /Type /XObject /Subtype /Image /Width "+
		itoa(w)+" /Height "+itoa(h)+" /BitsPerComponent 8 /ColorSpace "+cs+
		" /Filter /DCTDecode /Length "+itoa(len(jpegData))+" >>\nstream\n")...)
	raster = append(raster, jpegData...)
	raster = append(raster, "\nendstream"...)
	pdfBytes := testutil.PDFWithPageResources([]string{"q 100 0 0 100 0 0 cm /Photo Do Q"}, "/Photo 5 0 R", [][]byte{raster})
	source := &pdfSource{ctx: t.Context(), reader: bytes.NewReader(pdfBytes), remaining: 64 << 20}
	reader, err := pdf.NewReader(source, int64(len(pdfBytes)))
	require.NoError(t, err)
	imgVal := reader.Page(1).Resources().Key("XObject").Key("Photo")
	return testJPEGFixture{source: source, imgVal: imgVal, length: int64(len(jpegData))}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

func TestPDFJPEGReadsChargeSourceBudget(t *testing.T) {
	gray := encodeTestJPEG(t, image.NewGray(image.Rect(0, 0, 12, 12)))
	fx := pdfWithSingleJPEG(t, gray, 12, 12, "/DeviceGray")
	fx.source.remaining = fx.length / 2
	budget := int64(8 << 20)
	err := checkPDFImage(t.Context(), fx.source, fx.imgVal, pdf.Value{}, &budget, 0)
	require.Error(t, err)
	require.NotNil(t, fx.source.limitErr)
	require.ErrorContains(t, fx.source.limitErr, "maximum source read budget exceeded")
}

func TestPDFJPEGDoesNotUseDecodedBudget(t *testing.T) {
	gray := encodeTestJPEG(t, image.NewGray(image.Rect(0, 0, 100, 100)))
	fx := pdfWithSingleJPEG(t, gray, 100, 100, "/DeviceGray")
	const initialBudget int64 = 100 // smaller than 100*100 = 10,000 samples
	budget := initialBudget
	err := checkPDFImage(t.Context(), fx.source, fx.imgVal, pdf.Value{}, &budget, 0)
	require.NoError(t, err)
	require.Equal(t, initialBudget, budget)
}

func TestPDFJPEGHonorsCancellation(t *testing.T) {
	gray := encodeTestJPEG(t, image.NewGray(image.Rect(0, 0, 12, 12)))
	fx := pdfWithSingleJPEG(t, gray, 12, 12, "/DeviceGray")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	fx.source.ctx = ctx
	err := checkPDFJPEG(ctx, fx.source, fx.imgVal, fx.length, pdfJPEGFrame{width: 12, height: 12, components: 1})
	require.ErrorIs(t, err, context.Canceled)
}

func TestJPEGInEncryptedPDFIsRejected(t *testing.T) {
	source := &pdfSource{encrypted: true}
	err := checkPDFJPEG(t.Context(), source, pdf.Value{}, 10, pdfJPEGFrame{width: 1, height: 1, components: 1})
	require.EqualError(t, err, "JPEG image cannot be verified in an encrypted PDF")
}
