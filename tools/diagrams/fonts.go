package main

import (
	"embed"
	"fmt"
	"math"
	"strings"

	"golang.org/x/image/font"
	"golang.org/x/image/font/sfnt"
	"golang.org/x/image/math/fixed"
)

//go:embed fonts/*.ttf
var fontFiles embed.FS

type fontSet struct{ Regular, Bold *sfnt.Font }

func loadFonts() (*fontSet, error) {
	read := func(name string) (*sfnt.Font, error) {
		b, err := fontFiles.ReadFile("fonts/" + name)
		if err != nil {
			return nil, err
		}
		return sfnt.Parse(b)
	}
	r, err := read("Inter-Regular.ttf")
	if err != nil {
		return nil, err
	}
	b, err := read("Inter-SemiBold.ttf")
	if err != nil {
		return nil, err
	}
	return &fontSet{r, b}, nil
}
func (f *fontSet) face(bold bool) *sfnt.Font {
	if bold {
		return f.Bold
	}
	return f.Regular
}
func number(v float64) string {
	if math.Abs(v) < 0.0005 {
		v = 0
	}
	return strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.3f", v), "0"), ".")
}
func (f *fontSet) measure(l label) (box, error) {
	var buf sfnt.Buffer
	face := f.face(l.Bold)
	x := 0.0
	left, top, right, bottom := 0.0, 0.0, 0.0, 0.0
	for i, r := range l.Text {
		g, err := face.GlyphIndex(&buf, r)
		if err != nil || g == 0 {
			return box{}, fmt.Errorf("missing glyph %q", r)
		}
		bounds, advance, err := face.GlyphBounds(&buf, g, fixed.I(1000), font.HintingNone)
		if err != nil {
			return box{}, err
		}
		if i == 0 {
			left = float64(bounds.Min.X) / 64
			top = float64(bounds.Min.Y) / 64
			bottom = float64(bounds.Max.Y) / 64
		}
		left = math.Min(left, x+float64(bounds.Min.X)/64)
		right = math.Max(right, x+float64(bounds.Max.X)/64)
		top = math.Min(top, float64(bounds.Min.Y)/64)
		bottom = math.Max(bottom, float64(bounds.Max.Y)/64)
		x += float64(advance) / 64
		if l.Kind == "group" {
			x += 1000 * groupTracking
		}
	}
	// Advance widths reserve the full typographic cell, including spaces.
	right = math.Max(right, x)
	scale := l.Size / 1000
	return box{l.X + left*scale, l.Y + top*scale, (right - left) * scale, (bottom - top) * scale}, nil
}
func (f *fontSet) glyph(bold bool, r rune) (outline string, advanceWidth float64, err error) {
	var buf sfnt.Buffer
	face := f.face(bold)
	g, err := face.GlyphIndex(&buf, r)
	if err != nil || g == 0 {
		return "", 0, fmt.Errorf("missing glyph %q", r)
	}
	segments, err := face.LoadGlyph(&buf, g, fixed.I(1000), nil)
	if err != nil {
		return "", 0, err
	}
	var d strings.Builder
	p := func(v fixed.Point26_6) string {
		return number(math.Round(float64(v.X)/64)) + " " + number(math.Round(float64(v.Y)/64))
	}
	for _, s := range segments {
		switch s.Op {
		case sfnt.SegmentOpMoveTo:
			d.WriteString("M" + p(s.Args[0]))
		case sfnt.SegmentOpLineTo:
			d.WriteString("L" + p(s.Args[0]))
		case sfnt.SegmentOpQuadTo:
			d.WriteString("Q" + p(s.Args[0]) + " " + p(s.Args[1]))
		case sfnt.SegmentOpCubeTo:
			d.WriteString("C" + p(s.Args[0]) + " " + p(s.Args[1]) + " " + p(s.Args[2]))
		}
	}
	advance, err := face.GlyphAdvance(&buf, g, fixed.I(1000), font.HintingNone)
	return d.String(), float64(advance) / 64, err
}
func glyphID(bold bool, r rune) string {
	prefix := "r"
	if bold {
		prefix = "s"
	}
	return fmt.Sprintf("%s%x", prefix, r)
}
