package main

type point [2]float64

type box struct{ X, Y, W, H float64 }
type column struct{ Name, Type, Key, Reference string }
type node struct {
	Category    string
	Row, Column string
	Prefix      string
	Borderless  bool
	ID, Title   string
	Lines       []string
	Columns     []column
	Box         box
	Times       []float64
}
type label struct {
	Text        string
	X, Y, Size  float64
	Bold        bool
	Owner, Kind string
}
type edge struct {
	ID, From, To string
	Points       []point
	Label        *label
	Times        []float64
}
type group struct {
	Title    string
	Category string
	Box      box
}
type diagram struct {
	Name, Title, Description string
	W, H, Duration           float64
	Nodes                    []node
	Edges                    []edge
	Groups                   []group
	Labels                   []label
	MaxW, MaxH               float64
}
type placement struct {
	Width, Height float64
	Nodes         map[string]box
	Groups        []group
}
type sqlSchema map[string]map[string]column
