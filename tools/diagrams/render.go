package main

import (
	"cmp"
	"fmt"
	"html"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"
)

func roundedPath(points []point) string {
	p := func(v point) string { return number(v[0]) + " " + number(v[1]) }
	if len(points) == 0 {
		return ""
	}
	d := "M" + p(points[0])
	for i := 1; i < len(points)-1; i++ {
		prev, at, next := points[i-1], points[i], points[i+1]
		before, after := math.Hypot(at[0]-prev[0], at[1]-prev[1]), math.Hypot(next[0]-at[0], next[1]-at[1])
		if before == 0 || after == 0 {
			continue
		}
		r := math.Min(6, math.Min(before/2, after/2))
		a := point{at[0] + (prev[0]-at[0])*r/before, at[1] + (prev[1]-at[1])*r/before}
		b := point{at[0] + (next[0]-at[0])*r/after, at[1] + (next[1]-at[1])*r/after}
		d += "L" + p(a) + "Q" + p(at) + " " + p(b)
	}
	return d + "L" + p(points[len(points)-1])
}
func pulseDuration(length float64) float64 {
	return math.Max(pulseSeconds, (length+pulseDash+pulseStroke)/pulseSpeed)
}

// highlightSpan is how long one visit keeps a node lit, fades included.
const highlightSpan = 2*highlightFade + highlightHold

// loopDuration rounds the end of the last pulse or highlight, plus a rest, up
// to whole seconds.
func loopDuration(d diagram) float64 {
	end := 0.0
	for _, e := range d.Edges {
		for _, at := range e.Times {
			end = math.Max(end, at+pulseDuration(edgeLength(e)))
		}
	}
	for _, n := range d.Nodes {
		for _, at := range n.Times {
			end = math.Max(end, at+highlightSpan)
		}
	}
	return math.Ceil(end + restSeconds)
}

func animation(name string, times []float64, duration float64, pulse bool, length float64) string {
	var b strings.Builder
	start := number(pulseDash + pulseStroke/2)
	end := number(-(length + pulseStroke/2))
	fmt.Fprintf(&b, "@keyframes %s{0%%{opacity:0", name)
	if pulse {
		b.WriteString(";stroke-dashoffset:" + start)
	}
	b.WriteString("}")
	pct := func(t float64) string { return number(t*100/duration) + "%" }
	times = slices.Clone(times)
	slices.Sort(times)
	for i := 0; i < len(times); i++ {
		t := times[i]
		if pulse {
			travel := pulseDuration(length)
			fmt.Fprintf(&b, "%s{opacity:0;stroke-dashoffset:%s}%s{opacity:1;stroke-dashoffset:%s}%s{opacity:1;stroke-dashoffset:%s}%s{opacity:0;stroke-dashoffset:%s}%s{opacity:0;stroke-dashoffset:%s}", pct(t), start, pct(t+.001), start, pct(t+travel-.001), end, pct(t+travel), end, pct(t+travel+.001), start)
		} else {
			last := t
			for i+1 < len(times) && times[i+1] <= last+highlightSpan {
				i++
				last = times[i]
			}
			fmt.Fprintf(&b, "%s{opacity:0}%s{opacity:1}%s{opacity:1}%s{opacity:0}", pct(t), pct(t+highlightFade), pct(last+highlightFade+highlightHold), pct(last+highlightSpan))
		}
	}
	b.WriteString("100%{opacity:0")
	if pulse {
		b.WriteString(";stroke-dashoffset:" + start)
	}
	b.WriteString("}}")
	return b.String()
}

func render(d diagram, theme string, f *fontSet) ([]byte, error) {
	p, ok := palettes[theme]
	if !ok {
		return nil, fmt.Errorf("unknown theme %s", theme)
	}
	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %s %s" role="img" fill="%s"><title>%s</title><desc>%s</desc>`, number(d.W), number(d.H), p.Text, html.EscapeString(d.Title), html.EscapeString(d.Description))
	b.WriteString("<style>")
	for i, n := range d.Nodes {
		if len(n.Times) > 0 {
			b.WriteString(animation(fmt.Sprintf("n%d", i), n.Times, d.Duration, false, 1))
		}
	}
	for i, e := range d.Edges {
		if len(e.Times) > 0 {
			b.WriteString(animation(fmt.Sprintf("e%d", i), e.Times, d.Duration, true, edgeLength(e)))
		}
	}
	b.WriteString(`@media(prefers-reduced-motion:reduce){.pulse,.hit,.ink{animation:none!important;opacity:0!important}}</style><defs>`)
	labels := sceneLabels(d)
	glyphs := map[string]struct {
		bold bool
		r    rune
	}{}
	for _, l := range labels {
		for _, r := range l.Text {
			if r != ' ' {
				glyphs[glyphID(l.Bold, r)] = struct {
					bold bool
					r    rune
				}{l.Bold, r}
			}
		}
	}
	keys := make([]string, 0, len(glyphs))
	for id := range glyphs {
		keys = append(keys, id)
	}
	slices.Sort(keys)
	for _, id := range keys {
		g := glyphs[id]
		path, _, err := f.glyph(g.bold, g.r)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(&b, `<path id="%s" d="%s"/>`, id, path)
	}
	animatedTables := make(map[string]bool)
	for _, n := range d.Nodes {
		if len(n.Columns) > 0 && len(n.Times) > 0 {
			animatedTables[n.ID] = true
		}
	}
	// The normal and highlighted instances of an animated table's labels share one definition.
	labelIDs := make(map[int]string)
	for i, l := range labels {
		if animatedTables[l.Owner] && (l.Kind == "muted" || l.Kind == "accent") {
			labelIDs[i] = fmt.Sprintf("table-label-%d", i)
		}
	}
	// Text that is written more than once becomes a shared run of glyphs.
	type runKey struct {
		bold, group bool
		size        float64
		text        string
	}
	keyOf := func(l label) runKey { return runKey{l.Bold, l.Kind == "group", l.Size, l.Text} }
	runUses := make(map[runKey]int)
	for _, l := range labels {
		runUses[keyOf(l)]++
	}
	for _, n := range d.Nodes {
		if len(n.Times) == 0 {
			continue
		}
		for j, l := range labels {
			if l.Owner == n.ID && (l.Kind == "muted" || l.Kind == "accent") && labelIDs[j] == "" {
				runUses[keyOf(l)]++
			}
		}
	}
	var runKeys []runKey
	for key, uses := range runUses {
		if uses > 1 && utf8.RuneCountInString(key.text) > 1 {
			runKeys = append(runKeys, key)
		}
	}
	slices.SortFunc(runKeys, func(a, b runKey) int {
		return cmp.Or(strings.Compare(a.text, b.text), cmp.Compare(a.size, b.size), cmp.Compare(boolRank(a.bold), boolRank(b.bold)), cmp.Compare(boolRank(a.group), boolRank(b.group)))
	})
	writeRunGlyphs := func(out *strings.Builder, key runKey) error {
		x := 0.0
		for _, r := range key.text {
			_, advance, err := f.glyph(key.bold, r)
			if err != nil {
				return err
			}
			if r != ' ' {
				if x == 0 {
					fmt.Fprintf(out, `<use href="#%s"/>`, glyphID(key.bold, r))
				} else {
					fmt.Fprintf(out, `<use href="#%s" x="%s"/>`, glyphID(key.bold, r), number(math.Round(x)))
				}
			}
			x += advance
			if key.group {
				x += 1000 * groupTracking
			}
		}
		return nil
	}
	runIDs := make(map[runKey]string, len(runKeys))
	for i, key := range runKeys {
		id := fmt.Sprintf("run-%d", i)
		runIDs[key] = id
		fmt.Fprintf(&b, `<g id="%s" transform="scale(%s)">`, id, strconv.FormatFloat(key.size/1000, 'f', -1, 64))
		if err := writeRunGlyphs(&b, key); err != nil {
			return nil, err
		}
		b.WriteString("</g>")
	}
	fmt.Fprintf(&b, `<marker id="arrow" markerUnits="userSpaceOnUse" markerWidth="8" markerHeight="7" refX="8" refY="3.5" orient="auto"><path d="M0 0L8 3.5L0 7Z" fill="%s"/></marker></defs>`, p.Edge)
	rect := func(class string, r box, radius float64, extra string) {
		fmt.Fprintf(&b, `<rect class="%s" x="%s" y="%s" width="%s" height="%s" rx="%s"%s/>`, class, number(r.X), number(r.Y), number(r.W), number(r.H), number(radius), extra)
	}
	rect("background", box{0, 0, d.W, d.H}, groupRadius, ` fill="`+p.Page+`"`)
	if len(d.Groups) > 0 {
		fmt.Fprintf(&b, `<g fill="none" stroke="%s" stroke-width="%s" stroke-dasharray="4 4">`, p.Group, number(groupStroke))
		for _, g := range d.Groups {
			rect("group", g.Box, groupRadius, "")
		}
		b.WriteString("</g>")
	}
	if len(d.Edges) > 0 {
		fmt.Fprintf(&b, `<g fill="none" stroke="%s" stroke-width="%s" stroke-linejoin="round" marker-end="url(#arrow)">`, p.Edge, number(edgeStroke))
		for _, e := range d.Edges {
			fmt.Fprintf(&b, `<path class="edge" d="%s"/>`, roundedPath(e.Points))
		}
		b.WriteString("</g>")
		fmt.Fprintf(&b, `<g fill="none" stroke="%s" stroke-width="%s" stroke-linecap="round">`, p.Accent, number(pulseStroke))
		for i, e := range d.Edges {
			if len(e.Times) > 0 {
				fmt.Fprintf(&b, `<path class="pulse" opacity="0" stroke-dasharray="%s %s" d="%s" style="animation:e%d %ss %s infinite"/>`, number(pulseDash), number(2*edgeLength(e)+2*pulseDash+1), roundedPath(e.Points), i, number(d.Duration), easing)
			}
		}
		b.WriteString("</g>")
	}
	hasNodes, hasColumns := false, false
	for _, n := range d.Nodes {
		hasNodes = hasNodes || !n.Borderless
		hasColumns = hasColumns || len(n.Columns) > 0
	}
	if hasNodes {
		fmt.Fprintf(&b, `<g fill="%s" stroke="%s" stroke-width="%s">`, p.Surface, p.Border, number(nodeStroke))
		for _, n := range d.Nodes {
			if !n.Borderless {
				rect("node", n.Box, nodeRadius, "")
			}
		}
		b.WriteString("</g>")
	}
	for i, n := range d.Nodes {
		if len(n.Times) > 0 {
			fmt.Fprintf(&b, `<g fill="%s">`, p.Soft)
			rect("hit", grow(n.Box, -1), nodeRadius-1, fmt.Sprintf(` opacity="0" style="animation:n%d %ss %s infinite"`, i, number(d.Duration), easing))
			b.WriteString("</g>")
		}
	}
	if hasColumns {
		fmt.Fprintf(&b, `<g fill="none" stroke="%s" stroke-width="%s">`, p.Border, number(nodeStroke))
		for _, n := range d.Nodes {
			if len(n.Columns) > 0 {
				fmt.Fprintf(&b, `<path d="M%s %sH%s"/>`, number(n.Box.X), number(n.Box.Y+42), number(n.Box.X+n.Box.W))
			}
		}
		b.WriteString("</g>")
	}
	writeLabel := func(l label, id string) error {
		idAttr := ""
		if id != "" {
			idAttr = ` id="` + id + `"`
		}
		if runID, ok := runIDs[keyOf(l)]; ok {
			fmt.Fprintf(&b, `<use%s href="#%s" x="%s" y="%s"/>`, idAttr, runID, number(l.X), number(l.Y))
			return nil
		}
		fmt.Fprintf(&b, `<g%s transform="translate(%s %s) scale(%s)">`, idAttr, number(l.X), number(l.Y), strconv.FormatFloat(l.Size/1000, 'f', -1, 64))
		if err := writeRunGlyphs(&b, keyOf(l)); err != nil {
			return err
		}
		b.WriteString("</g>")
		return nil
	}

	fillOf := func(l label) string {
		switch l.Kind {
		case "muted", "group":
			return p.Muted
		case "accent":
			return p.Accent
		default:
			return p.Text
		}
	}
	for _, fill := range []string{p.Text, p.Muted, p.Accent} {
		opened := false
		for i, l := range labels {
			if fillOf(l) != fill {
				continue
			}
			if !opened {
				if fill == p.Text {
					b.WriteString("<g>")
				} else {
					fmt.Fprintf(&b, `<g fill="%s">`, fill)
				}
				opened = true
			}
			if err := writeLabel(l, labelIDs[i]); err != nil {
				return nil, err
			}
		}
		if opened {
			b.WriteString("</g>")
		}
	}
	for i, n := range d.Nodes {
		if len(n.Times) == 0 {
			continue
		}
		fmt.Fprintf(&b, `<g class="ink" opacity="0" fill="%s" style="animation:n%d %ss %s infinite">`, p.Text, i, number(d.Duration), easing)
		for j, l := range labels {
			if l.Owner == n.ID && (l.Kind == "muted" || l.Kind == "accent") {
				if id := labelIDs[j]; id != "" {
					fmt.Fprintf(&b, `<use href="#%s"/>`, id)
					continue
				}
				if err := writeLabel(l, ""); err != nil {
					return nil, err
				}
			}
		}
		b.WriteString("</g>")
	}
	b.WriteString("</svg>\n")
	return []byte(strings.ReplaceAll(b.String(), "><", ">\n<")), nil
}

func boolRank(v bool) int {
	if v {
		return 1
	}
	return 0
}

func edgeLength(e edge) float64 {
	length := 0.0
	for i := 1; i < len(e.Points); i++ {
		length += math.Hypot(e.Points[i][0]-e.Points[i-1][0], e.Points[i][1]-e.Points[i-1][1])
	}
	return length
}
