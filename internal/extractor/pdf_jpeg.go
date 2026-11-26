package extractor

import (
	"bufio"
	"errors"
	"io"
)

type pdfJPEGFrame struct {
	width, height, components int64
}

type pdfJPEGComponent struct {
	id   byte
	h, v int
	tq   byte
}

type pdfJPEGState struct {
	frame      pdfJPEGFrame
	sofMarker  byte
	hasFrame   bool
	hasScan    bool
	comps      [4]pdfJPEGComponent
	dqtDefined [4]bool
	dhtDefined [2][4]bool
}

// readPDFJPEGFrame validates a JPEG codestream's marker structure through EOI
// without decoding pixel samples (ITU-T T.81 Annex B).
func readPDFJPEGFrame(r io.Reader) (pdfJPEGFrame, error) {
	br := bufio.NewReaderSize(r, 32<<10)
	var soi [2]byte
	if err := readJPEGFull(br, soi[:]); err != nil {
		return pdfJPEGFrame{}, err
	}
	if soi[0] != 0xFF || soi[1] != 0xD8 {
		return pdfJPEGFrame{}, errors.New("missing start of image")
	}

	var state pdfJPEGState
	var code byte
	var haveCode bool

	for {
		if !haveCode {
			b, err := readJPEGByte(br)
			if err != nil {
				return pdfJPEGFrame{}, err
			}
			if b != 0xFF {
				return pdfJPEGFrame{}, errors.New("invalid marker")
			}
			for b == 0xFF {
				b, err = readJPEGByte(br)
				if err != nil {
					return pdfJPEGFrame{}, err
				}
			}
			code = b
		}
		haveCode = false

		switch code {
		case 0x00:
			return pdfJPEGFrame{}, errors.New("invalid marker")
		case 0xD8:
			return pdfJPEGFrame{}, errors.New("unexpected start of image")
		case 0xD9:
			if !state.hasFrame || !state.hasScan {
				return pdfJPEGFrame{}, errors.New("missing JPEG scan")
			}
			return state.frame, nil
		case 0x01, 0xD0, 0xD1, 0xD2, 0xD3, 0xD4, 0xD5, 0xD6, 0xD7:
			continue
		}

		segLen, err := readJPEGUint16(br)
		if err != nil {
			return pdfJPEGFrame{}, err
		}
		if segLen < 2 {
			return pdfJPEGFrame{}, errors.New("invalid segment length")
		}
		n := segLen - 2

		switch {
		case code == 0xC0 || code == 0xC1 || code == 0xC2:
			if segErr := state.readFrame(br, code, n); segErr != nil {
				return pdfJPEGFrame{}, segErr
			}
		case code == 0xC4:
			if segErr := state.readDHT(br, n); segErr != nil {
				return pdfJPEGFrame{}, segErr
			}
		case code == 0xDB:
			if segErr := state.readDQT(br, n); segErr != nil {
				return pdfJPEGFrame{}, segErr
			}
		case code == 0xDD:
			if n != 2 {
				return pdfJPEGFrame{}, errors.New("invalid restart interval length")
			}
			if segErr := discardJPEGBytes(br, n); segErr != nil {
				return pdfJPEGFrame{}, segErr
			}
		case code == 0xDC || (code >= 0xE0 && code <= 0xEF) || code == 0xFE:
			if segErr := discardJPEGBytes(br, n); segErr != nil {
				return pdfJPEGFrame{}, segErr
			}
		case code == 0xDA:
			if segErr := state.readSOS(br, n); segErr != nil {
				return pdfJPEGFrame{}, segErr
			}
			code, err = scanJPEGEntropy(br)
			if err != nil {
				return pdfJPEGFrame{}, err
			}
			haveCode = true
		case isUnsupportedJPEGCoding(code):
			return pdfJPEGFrame{}, errors.New("unsupported JPEG coding process")
		default:
			return pdfJPEGFrame{}, errors.New("unsupported JPEG marker")
		}
	}
}

func isUnsupportedJPEGCoding(code byte) bool {
	switch code {
	case 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCC, 0xCD, 0xCE, 0xCF, 0xDE, 0xDF:
		return true
	default:
		return false
	}
}

func (s *pdfJPEGState) readFrame(br *bufio.Reader, code byte, n int) error {
	if s.hasFrame {
		return errors.New("multiple frame headers")
	}
	if n < 6 {
		return errors.New("invalid frame header")
	}
	var hdr [6]byte
	if err := readJPEGFull(br, hdr[:]); err != nil {
		return err
	}
	if hdr[0] != 8 {
		return errors.New("unsupported JPEG sample precision")
	}
	height := int64(hdr[1])<<8 | int64(hdr[2])
	if height == 0 {
		return errors.New("JPEG height defined later is unsupported")
	}
	width := int64(hdr[3])<<8 | int64(hdr[4])
	if width == 0 {
		return errors.New("invalid JPEG width")
	}
	nf := int(hdr[5])
	if nf != 1 && nf != 3 && nf != 4 {
		return errors.New("unsupported JPEG component count")
	}
	if n != 6+3*nf {
		return errors.New("invalid frame header")
	}
	var compBuf [12]byte
	if err := readJPEGFull(br, compBuf[:3*nf]); err != nil {
		return err
	}
	for i := range nf {
		id := compBuf[3*i]
		for j := range i {
			if s.comps[j].id == id {
				return errors.New("duplicate JPEG component identifier")
			}
		}
		hv := compBuf[3*i+1]
		h, v := int(hv>>4), int(hv&0x0F)
		if h < 1 || h > 4 || v < 1 || v > 4 {
			return errors.New("invalid JPEG sampling factor")
		}
		tq := compBuf[3*i+2]
		if tq > 3 {
			return errors.New("invalid JPEG quantization table selector")
		}
		s.comps[i] = pdfJPEGComponent{id: id, h: h, v: v, tq: tq}
	}
	s.hasFrame = true
	s.sofMarker = code
	s.frame = pdfJPEGFrame{width: width, height: height, components: int64(nf)}
	return nil
}

func (s *pdfJPEGState) readDHT(br *bufio.Reader, n int) error {
	if n == 0 {
		return errors.New("invalid Huffman table length")
	}
	for n > 0 {
		if n < 17 {
			return errors.New("invalid Huffman table length")
		}
		var hdr [17]byte
		if err := readJPEGFull(br, hdr[:]); err != nil {
			return err
		}
		n -= 17
		tc, th := hdr[0]>>4, hdr[0]&0x0F
		if tc > 1 || th > 3 {
			return errors.New("invalid Huffman table selector")
		}
		total, code := 0, 0
		for i := 1; i <= 16; i++ {
			c := int(hdr[i])
			total += c
			code += c
			if code > 1<<i {
				return errors.New("invalid Huffman table")
			}
			code <<= 1
		}
		if total < 1 || total > 256 {
			return errors.New("invalid Huffman table")
		}
		if n < total {
			return errors.New("invalid Huffman table length")
		}
		if err := discardJPEGBytes(br, total); err != nil {
			return err
		}
		n -= total
		s.dhtDefined[tc][th] = true
	}
	return nil
}

func (s *pdfJPEGState) readDQT(br *bufio.Reader, n int) error {
	if n == 0 {
		return errors.New("invalid quantization table length")
	}
	for n > 0 {
		b, err := readJPEGByte(br)
		if err != nil {
			return err
		}
		n--
		pq, tq := b>>4, b&0x0F
		if pq > 1 || tq > 3 {
			return errors.New("invalid quantization table")
		}
		size := 64 * (int(pq) + 1)
		if n < size {
			return errors.New("invalid quantization table length")
		}
		if err := discardJPEGBytes(br, size); err != nil {
			return err
		}
		n -= size
		s.dqtDefined[tq] = true
	}
	return nil
}

func (s *pdfJPEGState) readSOS(br *bufio.Reader, n int) error {
	if !s.hasFrame {
		return errors.New("scan before frame header")
	}
	if n < 6 || n > 12 || n%2 != 0 {
		return errors.New("invalid scan header")
	}
	var buf [12]byte
	if err := readJPEGFull(br, buf[:n]); err != nil {
		return err
	}
	ns := int(buf[0])
	if ns < 1 || ns > 4 || n != 4+2*ns {
		return errors.New("invalid scan header")
	}
	ss, se := buf[1+2*ns], buf[2+2*ns]
	ah, al := buf[3+2*ns]>>4, buf[3+2*ns]&0x0F

	progressive := s.sofMarker == 0xC2
	if progressive {
		if ss > se || se > 63 || (ss == 0 && se != 0) || (ss > 0 && ns != 1) || ah > 13 || al > 13 {
			return errors.New("invalid progressive scan parameters")
		}
	} else if ss != 0 || se != 63 || ah != 0 || al != 0 {
		return errors.New("invalid sequential scan parameters")
	}

	prevIdx := -1
	totalHV := 0
	for i := range ns {
		cs := buf[1+2*i]
		compIdx := -1
		for j := range int(s.frame.components) {
			if s.comps[j].id == cs {
				compIdx = j
				break
			}
		}
		if compIdx < 0 {
			return errors.New("unknown scan component")
		}
		if compIdx <= prevIdx {
			return errors.New("invalid scan component order")
		}
		prevIdx = compIdx
		totalHV += s.comps[compIdx].h * s.comps[compIdx].v
		if !s.dqtDefined[s.comps[compIdx].tq] {
			return errors.New("undefined quantization table")
		}

		td, ta := buf[2+2*i]>>4, buf[2+2*i]&0x0F
		if err := s.checkScanTables(td, ta, progressive, ss, ah); err != nil {
			return err
		}
	}
	if ns > 1 && totalHV > 10 {
		return errors.New("invalid scan sampling factors")
	}
	s.hasScan = true
	return nil
}

func (s *pdfJPEGState) checkScanTables(td, ta byte, progressive bool, ss, ah byte) error {
	if !progressive {
		maxTable := byte(3)
		if s.sofMarker == 0xC0 {
			maxTable = 1
		}
		if td > maxTable || ta > maxTable {
			return errors.New("invalid Huffman table selector")
		}
		if !s.dhtDefined[0][td] || !s.dhtDefined[1][ta] {
			return errors.New("undefined Huffman table")
		}
		return nil
	}
	if ss == 0 {
		if ah == 0 {
			if td > 3 {
				return errors.New("invalid Huffman table selector")
			}
			if !s.dhtDefined[0][td] {
				return errors.New("undefined Huffman table")
			}
		}
		return nil
	}
	if ta > 3 {
		return errors.New("invalid Huffman table selector")
	}
	if !s.dhtDefined[1][ta] {
		return errors.New("undefined Huffman table")
	}
	return nil
}

func scanJPEGEntropy(br *bufio.Reader) (byte, error) {
	for {
		_, err := br.ReadSlice(0xFF)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			return 0, jpegReadErr(err)
		}
		b, err := readJPEGByte(br)
		if err != nil {
			return 0, err
		}
		for b == 0xFF {
			b, err = readJPEGByte(br)
			if err != nil {
				return 0, err
			}
		}
		if b == 0x00 || (b >= 0xD0 && b <= 0xD7) {
			continue
		}
		return b, nil
	}
}

func readJPEGByte(br *bufio.Reader) (byte, error) {
	b, err := br.ReadByte()
	if err != nil {
		return 0, jpegReadErr(err)
	}
	return b, nil
}

func readJPEGFull(br *bufio.Reader, buf []byte) error {
	if _, err := io.ReadFull(br, buf); err != nil {
		return jpegReadErr(err)
	}
	return nil
}

func readJPEGUint16(br *bufio.Reader) (int, error) {
	var buf [2]byte
	if err := readJPEGFull(br, buf[:]); err != nil {
		return 0, err
	}
	return int(buf[0])<<8 | int(buf[1]), nil
}

func discardJPEGBytes(br *bufio.Reader, n int) error {
	if n == 0 {
		return nil
	}
	if _, err := io.CopyN(io.Discard, br, int64(n)); err != nil {
		return jpegReadErr(err)
	}
	return nil
}

func jpegReadErr(err error) error {
	// Never wrap io.ErrUnexpectedEOF so the sandbox child reports a plain extraction error.
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errors.New("truncated JPEG data")
	}
	return err
}
