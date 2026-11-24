package extractor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Victoroide/quarry-rag/internal/domain"
	"github.com/ledongthuc/pdf"
)

// Verify the declared count against the tree before using it as a submission budget.
func readPDFPageTree(ctx context.Context, node pdf.Value, pages *[]pdf.Page, depth int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if depth > 64 || len(*pages) >= domain.MaxPDFPages || node.Kind() != pdf.Dict {
		return errors.New("extractor: pdf: invalid or over-budget page tree")
	}
	switch node.Key("Type").Name() {
	case "Page":
		if depth == 0 {
			return errors.New("extractor: pdf: missing page tree")
		}
		*pages = append(*pages, pdf.Page{V: node})
	case "Pages":
		count, kids := node.Key("Count").Int64(), node.Key("Kids")
		if count < 1 || count > domain.MaxPDFPages || kids.Kind() != pdf.Array || kids.Len() < 1 || int64(kids.Len()) > count {
			return errors.New("extractor: pdf: invalid or over-budget page count")
		}
		start := len(*pages)
		for i := 0; i < kids.Len(); i++ {
			if err := readPDFPageTree(ctx, kids.Index(i), pages, depth+1); err != nil {
				return err
			}
		}
		if int64(len(*pages)-start) != count {
			return errors.New("extractor: pdf: page count does not match page tree")
		}
	default:
		return errors.New("extractor: pdf: missing page in page tree")
	}
	return nil
}

type pdfPaint struct {
	text                       strings.Builder
	image, other, decodingLoss bool
	source                     *pdfSource
}

const (
	pdfAnnotationHidden = 1 << 1
	pdfAnnotationPrint  = 1 << 2
)

func (p *pdfPaint) inspectPage(ctx context.Context, page pdf.Page, contents pdf.Value, budget *int64) error {
	if !contents.IsNull() {
		if err := p.inspect(ctx, contents, page.Resources(), budget, 0, pdfGraphics{width: 1, scale: 1}); err != nil {
			return err
		}
	}
	annots := page.V.Key("Annots")
	if annots.Len() > 4096 {
		return errors.New("maximum annotation count exceeded")
	}
	for i := range annots.Len() {
		annotation := annots.Index(i)
		flags := annotation.Key("F").Int64()
		if flags&pdfAnnotationHidden != 0 {
			continue
		}
		if !annotation.Key("AP").IsNull() {
			p.other = true
		} else if flags&pdfAnnotationPrint != 0 && annotation.Key("Subtype").Name() == "FreeText" &&
			strings.TrimSpace(annotation.Key("Contents").Text()) != "" {
			// A reader can synthesize a text appearance from nonblank Contents and DA.
			// The rendered text is unresolved until detection.
			p.decodingLoss = true
		}
	}
	return nil
}

func (p *pdfPaint) evidence(text string) domain.PDFPageEvidence {
	switch {
	case p.decodingLoss || text == "" && p.other:
		return domain.PDFUnreadable
	case text == "" && p.image:
		return domain.PDFImage
	case text == "":
		return domain.PDFBlank
	case p.image || p.other:
		return domain.PDFMixed
	case !domain.PDFTextReadable(text):
		return domain.PDFSparse
	default:
		return domain.PDFEmbedded
	}
}

type pdfGraphics struct {
	font  pdf.TextEncoding
	width float64
	scale float64
}

// pdfFontEncoder returns nil for a missing font or a composite font without a
// Unicode map, which leaves every shown string undecodable.
func pdfFontEncoder(font pdf.Value) pdf.TextEncoding {
	if font.IsNull() || font.Key("Subtype").Name() == "Type0" && font.Key("ToUnicode").Kind() != pdf.Stream {
		return nil
	}
	return pdf.Font{V: font}.Encoder()
}

// pdfExtGState holds the parameters of a graphics state dictionary that the
// page classifier tracks: the font that Tf would select and the line width that
// w would set.
type pdfExtGState struct {
	font              pdf.TextEncoding
	width             float64
	hasFont, hasWidth bool
}

func (g pdfExtGState) apply(state *pdfGraphics) {
	if g.hasFont {
		state.font = g.font
	}
	if g.hasWidth {
		state.width = g.width
	}
}

// Fonts selected through graphics state dictionaries use the same bounds as the
// font resource dictionary.
const maxPDFGraphicsStateFonts = 128

// pdfGraphicsState resolves a named graphics state dictionary of the active
// resources. A selected font is validated against the work limits once per name.
func pdfGraphicsState(ctx context.Context, resources pdf.Value, name string, budget *int64, fonts map[string]pdfExtGState) (pdfExtGState, error) {
	if cached, found := fonts[name]; found {
		return cached, nil
	}
	dict := resources.Key("ExtGState").Key(name)
	if dict.Kind() != pdf.Dict {
		return pdfExtGState{}, errors.New("missing or invalid graphics state")
	}
	var selected pdfExtGState
	if width := dict.Key("LW"); !width.IsNull() {
		if !pdfNumbers([]pdf.Value{width}, 1, 1) {
			return pdfExtGState{}, errors.New("invalid graphics state line width")
		}
		selected.width, selected.hasWidth = width.Float64(), true
	}
	if entry := dict.Key("Font"); !entry.IsNull() {
		if entry.Kind() != pdf.Array || entry.Len() != 2 || entry.Index(0).Kind() != pdf.Dict || !pdfNumbers([]pdf.Value{entry.Index(1)}, 1, 1) {
			return pdfExtGState{}, errors.New("invalid graphics state font")
		}
		if len(fonts) >= maxPDFGraphicsStateFonts {
			return pdfExtGState{}, errors.New("maximum graphics state font count exceeded")
		}
		if err := checkPDFFont(ctx, entry.Index(0), budget); err != nil {
			return pdfExtGState{}, err
		}
		selected.font, selected.hasFont = pdfFontEncoder(entry.Index(0)), true
		fonts[name] = selected
	}
	return selected, nil
}

func (s *pdfGraphics) transform(a, b, c, d float64) {
	// Path classification needs the upper bound on transformed stroke width.
	s.scale *= max(math.Abs(a)+math.Abs(c), math.Abs(b)+math.Abs(d))
}

type pdfPath struct {
	x, y     float64
	moves    int
	segments int
	complex  bool
	rect     bool
}

func (p *pdfPath) operator(op string, args []pdf.Value, count int, scale float64) {
	switch op {
	case "m":
		p.moves++
		if pdfNumbers(args, count, 2) {
			p.x, p.y = args[0].Float64(), args[1].Float64()
		} else {
			p.complex = true
		}
	case "l":
		p.segments++
		if !pdfNumbers(args, count, 2) || p.moves != 1 || p.segments != 1 {
			p.complex = true
			return
		}
		x, y := args[0].Float64(), args[1].Float64()
		p.complex = p.complex || (p.x == x) == (p.y == y)
	case "c", "v", "y", "h":
		p.complex = true
	case "re":
		p.rect = true
		p.moves++
		p.segments++
		if !pdfNumbers(args, count, 4) || p.moves != 1 || p.segments != 1 {
			p.complex = true
			return
		}
		w, h := math.Abs(args[2].Float64()), math.Abs(args[3].Float64())
		p.complex = w == 0 || h == 0 || min(w, h)*scale > maxPDFRuleWidth
	}
}

const maxPDFRuleWidth = 3

func pdfNumbers(args []pdf.Value, count, want int) bool {
	if count != want {
		return false
	}
	for _, arg := range args[:want] {
		if arg.Kind() != pdf.Integer && arg.Kind() != pdf.Real {
			return false
		}
	}
	return true
}

func (p *pdfPaint) inspect(ctx context.Context, contents, resources pdf.Value, budget *int64, depth int, state pdfGraphics) error {
	if depth > 16 {
		return errors.New("maximum form nesting exceeded")
	}
	if contents.Kind() == pdf.Array {
		for i := range contents.Len() {
			if contents.Index(i).Kind() != pdf.Stream {
				return errors.New("invalid content stream array")
			}
		}
	} else if contents.Kind() != pdf.Stream {
		return errors.New("invalid content stream")
	}
	if err := checkPDFFonts(ctx, resources, budget); err != nil {
		return err
	}
	var err error
	var path pdfPath
	var saved []pdfGraphics
	encoders := make(map[string]pdf.TextEncoding)
	states := make(map[string]pdfExtGState)
	pdf.Interpret(contents, func(stack *pdf.Stack, op string) {
		if ctx.Err() != nil {
			panic(ctx.Err())
		}
		n := stack.Len()
		var args [6]pdf.Value
		for i := n - 1; i >= 0; i-- {
			arg := stack.Pop()
			if i < len(args) {
				args[i] = arg
			}
		}
		if err != nil {
			return
		}
		switch op {
		case "BT", "T*":
			p.text.WriteByte('\n')
		case "Tf":
			if n != 2 || args[0].Kind() != pdf.Name || !pdfNumbers(args[1:], n-1, 1) {
				err = errors.New("invalid font operands")
				return
			}
			name := args[0].Name()
			if _, found := encoders[name]; !found {
				encoders[name] = pdfFontEncoder(resources.Key("Font").Key(name))
			}
			state.font = encoders[name]
		case "gs":
			if n != 1 || args[0].Kind() != pdf.Name {
				err = errors.New("invalid graphics state operands")
				return
			}
			var selected pdfExtGState
			if selected, err = pdfGraphicsState(ctx, resources, args[0].Name(), budget, states); err == nil {
				selected.apply(&state)
			}
		case "Tj", "'", "\"", "TJ":
			err = p.showText(op, args[:], n, state.font)
		case "Do":
			if n != 1 || args[0].Kind() != pdf.Name {
				err = errors.New("invalid painted object reference")
				return
			}
			err = p.paintObject(ctx, resources, args[0].Name(), budget, depth, state)
		case "BI":
			err = errors.New("inline image cannot be inspected safely")
		case "q":
			if len(saved) >= 64 {
				err = errors.New("maximum graphics state nesting exceeded")
				return
			}
			saved = append(saved, state)
		case "Q":
			if len(saved) > 0 {
				state = saved[len(saved)-1]
				saved = saved[:len(saved)-1]
			}
		case "w":
			if pdfNumbers(args[:], n, 1) {
				state.width = args[0].Float64()
			}
		case "cm":
			if pdfNumbers(args[:], n, 6) {
				state.transform(args[0].Float64(), args[1].Float64(), args[2].Float64(), args[3].Float64())
			}
		case "m", "l", "c", "v", "y", "h", "re":
			path.operator(op, args[:], n, state.scale)
		case "S", "s":
			p.other = p.other || path.complex || path.moves != 1 || path.segments != 1 || state.width < 0 || state.width*state.scale > maxPDFRuleWidth
			path = pdfPath{}
		case "f", "F", "f*", "B", "B*", "b", "b*":
			p.other = p.other || path.complex || path.moves != 1 || path.segments != 1 || !path.rect
			path = pdfPath{}
		case "n":
			path = pdfPath{}
		case "sh":
			p.other = true
		}
		if p.text.Len() > domain.MaxContentBytes+8<<20 {
			err = fmt.Errorf("maximum text size exceeded: %w", ErrContentTooLarge)
		}
	})
	return err
}

func (p *pdfPaint) showText(op string, args []pdf.Value, n int, font pdf.TextEncoding) error {
	switch op {
	case "TJ":
		if n != 1 || args[0].Kind() != pdf.Array {
			return errors.New("invalid text array")
		}
		for i := range args[0].Len() {
			item := args[0].Index(i)
			switch item.Kind() {
			case pdf.String:
				p.decodeText(item.RawString(), font)
			case pdf.Integer, pdf.Real:
			default:
				return errors.New("invalid text array element")
			}
		}
	case "\"":
		if n != 3 || !pdfNumbers(args, 2, 2) || args[2].Kind() != pdf.String {
			return errors.New("invalid double-quote text operands")
		}
		p.text.WriteByte('\n')
		p.decodeText(args[2].RawString(), font)
	default:
		if n != 1 || args[0].Kind() != pdf.String {
			return errors.New("invalid text operands")
		}
		if op == "'" {
			p.text.WriteByte('\n')
		}
		p.decodeText(args[0].RawString(), font)
	}
	return nil
}

func (p *pdfPaint) decodeText(raw string, font pdf.TextEncoding) {
	if raw == "" {
		return
	}
	if font == nil {
		p.decodingLoss = true
		return
	}
	decoded := font.Decode(raw)
	if decoded == "" {
		p.decodingLoss = true
	}
	for _, r := range decoded {
		switch {
		case r == utf8.RuneError || (unicode.IsControl(r) && r != '\t' && r != '\n' && r != '\r'):
			p.decodingLoss = true
		default:
			p.text.WriteRune(r)
		}
	}
}

func (p *pdfPaint) paintObject(ctx context.Context, resources pdf.Value, name string, budget *int64, depth int, state pdfGraphics) error {
	object := resources.Key("XObject").Key(name)
	if object.Kind() != pdf.Stream {
		return errors.New("missing or invalid painted object")
	}
	switch object.Key("Subtype").Name() {
	case "Image":
		if err := checkPDFImage(ctx, p.source, object, resources, budget, 0); err != nil {
			return err
		}
		p.image = true
		return nil
	case "Form":
		if err := checkPDFStream(ctx, object, budget, 0); err != nil {
			return err
		}
		formResources := object.Key("Resources")
		if formResources.IsNull() {
			formResources = resources
		}
		if matrix := object.Key("Matrix"); !matrix.IsNull() {
			if matrix.Kind() != pdf.Array || matrix.Len() != 6 {
				return errors.New("invalid form matrix")
			}
			var args [6]pdf.Value
			for i := range args {
				args[i] = matrix.Index(i)
			}
			if !pdfNumbers(args[:], len(args), 6) {
				return errors.New("invalid form matrix")
			}
			state.transform(args[0].Float64(), args[1].Float64(), args[2].Float64(), args[3].Float64())
		}
		return p.inspect(ctx, object, formResources, budget, depth+1, state)
	default:
		return errors.New("unsupported painted object")
	}
}
