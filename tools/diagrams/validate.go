package main

import (
	"fmt"
	"math"
	"strings"
)

func intersects(a, b box) bool {
	return a.X < b.X+b.W && a.X+a.W > b.X && a.Y < b.Y+b.H && a.Y+a.H > b.Y
}
func grow(b box, n float64) box { return box{b.X - n, b.Y - n, b.W + 2*n, b.H + 2*n} }
func contains(a, b box) bool    { return b.X > a.X && b.Y > a.Y && b.X+b.W < a.X+a.W && b.Y+b.H < a.Y+a.H }
func segmentHits(a, b point, r box) bool {
	if a[0] == b[0] {
		return a[0] >= r.X && a[0] <= r.X+r.W && math.Max(a[1], b[1]) >= r.Y && math.Min(a[1], b[1]) <= r.Y+r.H
	}
	return a[1] >= r.Y && a[1] <= r.Y+r.H && math.Max(a[0], b[0]) >= r.X && math.Min(a[0], b[0]) <= r.X+r.W
}
func segmentsCross(a, b, c, d point) bool {
	if a[0] == b[0] && c[0] == d[0] {
		return a[0] == c[0] && math.Max(math.Min(a[1], b[1]), math.Min(c[1], d[1])) <= math.Min(math.Max(a[1], b[1]), math.Max(c[1], d[1]))
	}
	if a[1] == b[1] && c[1] == d[1] {
		return a[1] == c[1] && math.Max(math.Min(a[0], b[0]), math.Min(c[0], d[0])) <= math.Min(math.Max(a[0], b[0]), math.Max(c[0], d[0]))
	}
	if a[1] == b[1] {
		a, b, c, d = c, d, a, b
	}
	return a[0] >= math.Min(c[0], d[0]) && a[0] <= math.Max(c[0], d[0]) && c[1] >= math.Min(a[1], b[1]) && c[1] <= math.Max(a[1], b[1])
}
func onBorder(p point, b box) bool {
	return (p[0] == b.X || p[0] == b.X+b.W) && p[1] >= b.Y && p[1] <= b.Y+b.H || (p[1] == b.Y || p[1] == b.Y+b.H) && p[0] >= b.X && p[0] <= b.X+b.W
}
func validate(d diagram, f *fontSet) error {
	var errors []string
	fail := func(format string, args ...any) { errors = append(errors, fmt.Sprintf(format, args...)) }
	if d.W > 960 || d.MaxW > 0 && d.W > d.MaxW {
		fail("viewBox width exceeds readability limit")
	}
	if d.MaxH > 0 && d.H > d.MaxH {
		fail("viewBox height exceeds readability limit")
	}
	canvas := box{0, 0, d.W, d.H}
	nodes := map[string]node{}
	for i, n := range d.Nodes {
		if _, ok := nodes[n.ID]; ok {
			fail("duplicate node %s", n.ID)
		}
		nodes[n.ID] = n
		if !withinMargin(canvas, n.Box, 16) {
			fail("node %s outside viewBox", n.ID)
		}
		for _, other := range d.Nodes[:i] {
			a, b := n.Box, other.Box
			if !n.Borderless {
				a = grow(a, nodeStroke/2)
			}
			if !other.Borderless {
				b = grow(b, nodeStroke/2)
			}
			if intersects(a, b) {
				fail("nodes %s and %s overlap", n.ID, other.ID)
			}
		}
	}
	for _, e := range d.Edges {
		if d.Name == "system-architecture" && edgeLength(e) > 450 {
			fail("edge %s exceeds 450 units", e.ID)
		}
		if len(e.Points) < 2 {
			fail("edge %s needs at least two points", e.ID)
			continue
		}
		for _, end := range []struct {
			id string
			p  point
		}{{e.From, e.Points[0]}, {e.To, e.Points[len(e.Points)-1]}} {
			if end.id != "" {
				n, ok := nodes[end.id]
				if !ok || !onBorder(end.p, n.Box) {
					fail("edge %s endpoint is not on %s border", e.ID, end.id)
				} else {
					for _, corner := range []point{{n.Box.X, n.Box.Y}, {n.Box.X + n.Box.W, n.Box.Y}, {n.Box.X, n.Box.Y + n.Box.H}, {n.Box.X + n.Box.W, n.Box.Y + n.Box.H}} {
						if math.Hypot(end.p[0]-corner[0], end.p[1]-corner[1]) < nodeRadius+4 {
							fail("edge %s port is too close to node corner", e.ID)
						}
					}
				}
			}
		}
		for i := 1; i < len(e.Points); i++ {
			a, b := e.Points[i-1], e.Points[i]
			if a[0] != b[0] && a[1] != b[1] {
				fail("edge %s is not orthogonal", e.ID)
				continue
			}
			if !withinMargin(canvas, box{a[0], a[1], 0, 0}, 16) || !withinMargin(canvas, box{b[0], b[1], 0, 0}, 16) {
				fail("edge %s violates viewBox margin", e.ID)
			}
			for _, n := range d.Nodes {
				r := grow(n.Box, edgeClearance+edgeStroke/2)
				if n.ID == e.From || n.ID == e.To {
					r = grow(n.Box, -0.01)
				}
				if segmentHits(a, b, r) {
					fail("edge %s violates clearance of node %s", e.ID, n.ID)
				}
			}
		}
	}
	labels := sceneLabels(d)
	bounds := make([]box, len(labels))
	for i, l := range labels {
		if l.Size*880/d.W < 12 {
			fail("label %q is below 12 px at 880 px inspection width", l.Text)
		}
		b, err := f.measure(l)
		if err != nil {
			fail("%s: %v", l.Text, err)
			continue
		}
		bounds[i] = b
		if !withinMargin(canvas, b, 16) {
			fail("label %q violates viewBox margin", l.Text)
		}
		for _, n := range d.Nodes {
			if n.ID == l.Owner {
				if !contains(grow(n.Box, -nodeStroke/2-3), b) {
					fail("text %q overflows or touches its node border", l.Text)
				}
			} else if intersects(grow(b, 2), grow(n.Box, nodeStroke/2)) {
				fail("label %q overlaps node %s", l.Text, n.ID)
			}
		}
		for j := 0; j < i; j++ {
			if intersects(grow(b, 3), grow(bounds[j], 3)) {
				fail("labels %q and %q overlap", l.Text, labels[j].Text)
			}
		}
		for _, e := range d.Edges {
			for j := 1; j < len(e.Points); j++ {
				if segmentHits(e.Points[j-1], e.Points[j], grow(b, edgeStroke/2+6)) {
					fail("label %q overlaps edge %s", l.Text, e.ID)
				}
			}
		}
		for _, e := range d.Edges {
			if len(e.Points) < 2 {
				continue
			}
			tip := e.Points[len(e.Points)-1]
			prev := e.Points[len(e.Points)-2]
			length := math.Hypot(tip[0]-prev[0], tip[1]-prev[1])
			if length > 0 {
				base := point{tip[0] + (prev[0]-tip[0])*math.Min(8, length)/length, tip[1] + (prev[1]-tip[1])*math.Min(8, length)/length}
				if segmentHits(base, tip, grow(b, 3.5+6)) {
					fail("label %q overlaps edge %s arrowhead", l.Text, e.ID)
				}
			}
		}
		for _, g := range d.Groups {
			if intersects(b, g.Box) && !contains(grow(g.Box, -groupStroke/2-2), b) {
				fail("label %q touches a group border", l.Text)
			}
		}
	}

	ports := map[point]string{}
	edgeIDs := map[string]bool{}
	for _, e := range d.Edges {
		if edgeIDs[e.ID] {
			fail("duplicate edge %s", e.ID)
		}
		edgeIDs[e.ID] = true
		if len(e.Points) < 2 {
			continue
		}
		for _, port := range []struct {
			id string
			p  point
		}{{e.From, e.Points[0]}, {e.To, e.Points[len(e.Points)-1]}} {
			if port.id == "" {
				continue
			}
			if previous, ok := ports[port.p]; ok {
				fail("edges %s and %s share a node port", previous, e.ID)
			}
			ports[port.p] = e.ID
		}
		for _, g := range d.Groups {
			inside := contains(g.Box, box{e.Points[0][0], e.Points[0][1], 0, 0}) || contains(g.Box, box{e.Points[len(e.Points)-1][0], e.Points[len(e.Points)-1][1], 0, 0})
			if inside {
				continue
			}
			for j := 1; j < len(e.Points); j++ {
				if segmentHits(e.Points[j-1], e.Points[j], g.Box) {
					fail("edge %s crosses unrelated group %s", e.ID, g.Title)
				}
			}
		}
	}
	for _, n := range d.Nodes {
		for _, g := range d.Groups {
			if contains(g.Box, n.Box) && n.Category != g.Category {
				fail("node %s category differs from group %s", n.ID, g.Title)
			}
		}
	}
	for i, e := range d.Edges {
		for _, other := range d.Edges[:i] {
			for a := 1; a < len(e.Points); a++ {
				for b := 1; b < len(other.Points); b++ {
					u, v, w, z := e.Points[a-1], e.Points[a], other.Points[b-1], other.Points[b]
					if segmentsCross(u, v, w, z) {
						fail("edges %s and %s cross or overlap", e.ID, other.ID)
						continue
					}
					vertical := u[0] == v[0] && w[0] == z[0]
					horizontal := u[1] == v[1] && w[1] == z[1]
					if vertical && u[0] == w[0] || horizontal && u[1] == w[1] {
						gap := math.Max(math.Min(u[0], v[0]), math.Min(w[0], z[0])) - math.Min(math.Max(u[0], v[0]), math.Max(w[0], z[0]))
						if vertical {
							gap = math.Max(math.Min(u[1], v[1]), math.Min(w[1], z[1])) - math.Min(math.Max(u[1], v[1]), math.Max(w[1], z[1]))
						}
						if gap < parallelClearance {
							fail("edges %s and %s are collinear with gap under 12", e.ID, other.ID)
						}
					}
					tooClose := vertical && math.Abs(u[0]-w[0]) < parallelClearance && math.Max(math.Min(u[1], v[1]), math.Min(w[1], z[1])) <= math.Min(math.Max(u[1], v[1]), math.Max(w[1], z[1])) || horizontal && math.Abs(u[1]-w[1]) < parallelClearance && math.Max(math.Min(u[0], v[0]), math.Min(w[0], z[0])) <= math.Min(math.Max(u[0], v[0]), math.Max(w[0], z[0]))
					if tooClose {
						fail("parallel edges %s and %s are closer than 12 px", e.ID, other.ID)
					}
				}
			}
		}
	}

	columns := map[string]float64{}
	rows := map[string]float64{}
	for _, n := range d.Nodes {
		for _, axis := range []struct {
			name, key string
			value     float64
			set       map[string]float64
		}{{"column", n.Column, n.Box.X + n.Box.W/2, columns}, {"row", n.Row, n.Box.Y + n.Box.H/2, rows}} {
			if axis.key == "" {
				continue
			}
			if v, ok := axis.set[axis.key]; ok && math.Abs(v-axis.value) > .5 {
				fail("%s alignment differs for node %s", axis.name, n.ID)
			} else if !ok {
				axis.set[axis.key] = axis.value
			}
		}
	}
	for _, e := range d.Edges {
		a, ok := nodes[e.From]
		b, ok2 := nodes[e.To]
		if !ok || !ok2 || a.Row == "" || a.Row != b.Row || len(e.Points) != 2 {
			continue
		}
		center := a.Box.Y + a.Box.H/2
		for _, p := range e.Points {
			if math.Abs(p[1]-center) > .5 {
				fail("row alignment differs for edge %s", e.ID)
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("%s layout:\n%s", d.Name, strings.Join(errors, "\n"))
	}
	return nil
}

func withinMargin(canvas, b box, margin float64) bool {
	return b.X >= canvas.X+margin && b.Y >= canvas.Y+margin && b.X+b.W <= canvas.X+canvas.W-margin && b.Y+b.H <= canvas.Y+canvas.H-margin
}
