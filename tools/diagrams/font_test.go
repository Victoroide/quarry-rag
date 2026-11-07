package main

import (
	"crypto/sha256"
	"fmt"
	"math"
	"testing"
)

func TestBundledFontIntegrity(t *testing.T) {
	hashes := map[string]string{"Inter-Regular.ttf": "40d692fce188e4471e2b3cba937be967878f631ad3ebbbdcd587687c7ebe0c82", "Inter-SemiBold.ttf": "78a843fade9d4612a5567302fb595b56976eb5fcebf4fea5a5912d638bafcde3"}
	for name, want := range hashes {
		b, err := fontFiles.ReadFile("fonts/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256(b)); got != want {
			t.Errorf("%s hash changed: %s", name, got)
		}
	}
	f, err := loadFonts()
	if err != nil {
		t.Fatal(err)
	}
	wide, err := f.measure(label{Text: "WWW", Size: 14})
	if err != nil {
		t.Fatal(err)
	}
	narrow, err := f.measure(label{Text: "iii", Size: 14})
	if err != nil {
		t.Fatal(err)
	}
	if wide.W < 2*narrow.W {
		t.Fatal("glyph advances do not reflect character widths")
	}
}

func TestTextContrast(t *testing.T) {
	rgb := func(hex string) [3]float64 {
		var r, g, b uint8
		if _, err := fmt.Sscanf(hex, "#%02x%02x%02x", &r, &g, &b); err != nil {
			t.Fatal(err)
		}
		return [3]float64{float64(r) / 255, float64(g) / 255, float64(b) / 255}
	}
	blend := func(a, b [3]float64, alpha float64) [3]float64 {
		for i := range a {
			a[i] = a[i]*(1-alpha) + b[i]*alpha
		}
		return a
	}
	luminance := func(c [3]float64) float64 {
		for i, v := range c {
			if v <= .04045 {
				c[i] = v / 12.92
			} else {
				c[i] = math.Pow((v+.055)/1.055, 2.4)
			}
		}
		return .2126*c[0] + .7152*c[1] + .0722*c[2]
	}
	check := func(theme, fg, bg string, a, b [3]float64, alpha float64) {
		x, y := luminance(a), luminance(b)
		if x < y {
			x, y = y, x
		}
		if ratio := (x + .05) / (y + .05); ratio < 4.5 {
			t.Errorf("%s %s on %s opacity %.3f contrast %.3f", theme, fg, bg, alpha, ratio)
		}
	}
	for theme, p := range palettes {
		page := "#ffffff"
		if theme == "dark" {
			page = "#0d1117"
		}
		for _, fg := range []string{p.Text, p.Muted, p.Accent} {
			for _, bg := range []string{page, p.Surface} {
				check(theme, fg, bg, rgb(fg), rgb(bg), 0)
			}
			// Text changes to primary with the same opacity as the fill underneath it.
			// Every fade level must pass, including both complete highlighted states.
			for _, base := range []string{page, p.Surface} {
				for i := 0; i <= 1000; i++ {
					alpha := float64(i) / 1000
					check(theme, fg, "highlight on "+base, blend(rgb(fg), rgb(p.Text), alpha), blend(rgb(base), rgb(p.Soft), alpha), alpha)
				}
			}
		}
	}
}

func TestHighlightContrast(t *testing.T) {
	lum := func(hex string) float64 {
		var r, g, b uint8
		if _, err := fmt.Sscanf(hex, "#%02x%02x%02x", &r, &g, &b); err != nil {
			t.Fatal(err)
		}
		c := func(x uint8) float64 {
			v := float64(x) / 255
			if v <= .04045 {
				return v / 12.92
			}
			return math.Pow((v+.055)/1.055, 2.4)
		}
		return .2126*c(r) + .7152*c(g) + .0722*c(b)
	}
	for name, p := range palettes {
		a, b := lum(p.Soft), lum(p.Surface)
		if a < b {
			a, b = b, a
		}
		// The light highlight measures 1.175:1 against the node surface.
		if (a+.05)/(b+.05) < 1.15 {
			t.Errorf("%s highlight contrast %.3f", name, (a+.05)/(b+.05))
		}
	}
}
