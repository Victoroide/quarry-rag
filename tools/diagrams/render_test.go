package main

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestPulseHasOneDash(t *testing.T) {
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	for _, length := range []float64{24, 220, 450} {
		d := diagram{W: 840, H: 760, Duration: 16, Edges: []edge{{ID: "flow", Points: []point{{16, 100}, {16 + length, 100}}, Times: []float64{.1}}}}
		raw, err := render(d, "light", f)
		if err != nil {
			t.Fatal(err)
		}
		m := regexp.MustCompile(`class="pulse"[^>]*stroke-dasharray="([.0-9]+) ([.0-9]+)"`).FindStringSubmatch(string(raw))
		if len(m) != 3 {
			t.Fatal("pulse needs its own fixed dash and gap")
		}
		dash, _ := strconv.ParseFloat(m[1], 64)
		gap, _ := strconv.ParseFloat(m[2], 64)
		if dash != 12 || gap <= 2*length+2*dash {
			t.Fatalf("dash %g gap %g length %g", dash, gap, length)
		}
	}
}

func TestStaticPresentationAttributes(t *testing.T) {
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		for _, theme := range []string{"light", "dark"} {
			raw, err := render(d, theme, f)
			if err != nil {
				t.Fatal(err)
			}
			dec := xml.NewDecoder(strings.NewReader(string(raw)))
			for {
				tok, err := dec.Token()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				el, ok := tok.(xml.StartElement)
				if !ok {
					continue
				}
				attrs := map[string]string{}
				for _, a := range el.Attr {
					attrs[a.Name.Local] = a.Value
				}
				for _, class := range strings.Fields(attrs["class"]) {
					if class == "pulse" || class == "hit" || class == "ink" {
						if attrs["opacity"] != "0" {
							t.Errorf("%s %s lacks static opacity", d.Name, class)
						}
					}
				}
				if el.Name.Local == "style" {
					var css string
					if err := dec.DecodeElement(&css, &el); err != nil {
						t.Fatal(err)
					}
					css = regexp.MustCompile(`@keyframes [^{]+\{(?:[^{}]*\{[^{}]*\})+\}`).ReplaceAllString(css, "")
					want := `@media(prefers-reduced-motion:reduce){.pulse,.hit,.ink{animation:none!important;opacity:0!important}}`
					if css != want {
						t.Errorf("unexpected stylesheet rules: %s", css)
					}
				}
			}
		}
	}
}

func TestHighlightInkUsesMatchingTiming(t *testing.T) {
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		raw, err := render(d, "dark", f)
		if err != nil {
			t.Fatal(err)
		}
		for i, n := range d.Nodes {
			if len(n.Times) == 0 {
				continue
			}
			expected := fmt.Sprintf(`class="ink" opacity="0" fill=%q style="animation:n%d %ss %s infinite"`, palettes["dark"].Text, i, number(d.Duration), easing)
			if !strings.Contains(string(raw), expected) {
				t.Errorf("%s missing primary text fade for %s", d.Name, n.ID)
			}
		}
	}
}

func TestPulseReturnsToIdleOffset(t *testing.T) {
	css := animation("pulse", []float64{0.1, 8.1}, 16, true, 25)
	// The final hidden keyframe must restore the initial dash offset, so no
	// interpolation continues throughout the idle interval before the loop ends.
	frames := regexp.MustCompile(`([0-9.]+)%\{opacity:0;stroke-dashoffset:([^}]+)\}`).FindAllStringSubmatch(css, -1)
	if len(frames) < 2 || frames[len(frames)-1][2] != frames[0][2] {
		t.Fatal("invisible pulse keeps interpolating while idle")
	}
}

func TestHighlightWindowsAreOrderedAndContinuous(t *testing.T) {
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	// Overlapping visits stay lit between their peaks, even when supplied out of order.
	times := []float64{.44, .1, .78}
	css := animation("visits", times, 16, false, 1)
	frames := regexp.MustCompile(`([0-9.]+)%\{opacity:([01])`).FindAllStringSubmatch(css, -1)
	previous := -1.0
	for _, m := range frames {
		at, _ := strconv.ParseFloat(m[1], 64)
		if at <= previous {
			t.Errorf("offset %g follows %g", at, previous)
		}
		previous = at
		if m[2] == "0" && at > 100*.5/16 && at < 100*1.18/16 {
			t.Error("overlapping visits blink")
		}
	}
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		for i, n := range d.Nodes {
			times := append([]float64(nil), n.Times...)
			slices.Sort(times)
			css := animation(fmt.Sprintf("n%d", i), times, d.Duration, false, 1)
			for j := 1; j < len(times); j++ {
				if times[j]-times[j-1] > highlightSpan {
					continue
				}
				for _, frame := range regexp.MustCompile(`([0-9.]+)%\{opacity:0`).FindAllStringSubmatch(css, -1) {
					at, _ := strconv.ParseFloat(frame[1], 64)
					at = at * d.Duration / 100
					if at > times[j-1]+highlightFade && at < times[j]+highlightFade {
						t.Errorf("%s %s blinks between overlapping visits", d.Name, n.ID)
					}
				}
			}
		}
		raw, err := render(d, "light", f)
		if err != nil {
			t.Fatal(err)
		}
		for _, block := range regexp.MustCompile(`@keyframes [^{]+\{(?:[^{}]*\{[^{}]*\})+\}`).FindAllString(string(raw), -1) {
			previous = -1
			for _, m := range regexp.MustCompile(`([0-9.]+)%`).FindAllStringSubmatch(block, -1) {
				at, _ := strconv.ParseFloat(m[1], 64)
				if at <= previous {
					t.Errorf("%s unordered keyframe: %s", d.Name, block)
				}
				previous = at
			}
		}
	}
}

func TestHighlightHoldsBeforeFading(t *testing.T) {
	css := animation("visit", []float64{.1}, 16, false, 1)
	frames := regexp.MustCompile(`([0-9.]+)%\{opacity:([01])\}`).FindAllStringSubmatch(css, -1)
	var got []string
	for _, m := range frames {
		at, _ := strconv.ParseFloat(m[1], 64)
		got = append(got, fmt.Sprintf("%s@%.2f", m[2], at*16/100))
	}
	// Fade in over .4 s, stay lit for .8 s, then fade out over .4 s.
	want := []string{"0@0.00", "0@0.10", "1@0.50", "1@1.30", "0@1.70", "0@16.00"}
	if !slices.Equal(got, want) {
		t.Fatalf("visit frames %v, want %v", got, want)
	}
}

func TestVariantHasOpaqueRoundedBackground(t *testing.T) {
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	for theme, color := range map[string]string{"light": "#ffffff", "dark": "#0d1117"} {
		raw, err := render(diagram{W: 840, H: 760}, theme, f)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), `class="background" x="0" y="0" width="840" height="760" rx="14" fill="`+color+`"`) {
			t.Errorf("%s background missing", theme)
		}
	}
}

func TestPulseDurationAndTargetHighlightOffset(t *testing.T) {
	for _, length := range []float64{24, 360, 450, 720} {
		css := animation("travel", []float64{.1}, 16, true, length)
		frames := regexp.MustCompile(`([0-9.]+)%\{opacity:1;stroke-dashoffset:([-+.0-9]+)`).FindAllStringSubmatch(css, -1)
		if len(frames) != 2 {
			t.Fatalf("unexpected travel frames %s", css)
		}
		end, _ := strconv.ParseFloat(frames[1][1], 64)
		want := math.Max(.8, (length+14.5)/280)
		if math.Abs(end*16/100+.001-.1-want) > .0001 {
			t.Errorf("length %g duration is not distance/speed with minimum", length)
		}
	}
	ds, err := buildDiagrams("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		for _, e := range d.Edges {
			for _, at := range e.Times {
				want := at + math.Max(.8, (edgeLength(e)+14.5)/280) + .1
				found := false
				for _, n := range d.Nodes {
					if n.ID == e.To {
						for _, hit := range n.Times {
							if math.Abs(hit-want) < .00001 {
								found = true
							}
						}
					}
				}
				if !found {
					t.Errorf("%s target highlight must start .1 s after arrival", e.ID)
				}
			}
		}
	}
}
