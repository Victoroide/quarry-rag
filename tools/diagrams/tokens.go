package main

type palette struct{ Surface, Border, Edge, Group, Text, Muted, Accent, Soft, Page string }

// Opaque page colors preserve legibility when the image and page themes differ.
// The page background uses the same rounded corners as the groups.
var palettes = map[string]palette{
	"light": {"#f6f8fa", "#d0d7de", "#8c959f", "#d8dee4", "#1f2328", "#59636e", "#0969da", "#d4e8ff", "#ffffff"},
	"dark":  {"#161b22", "#30363d", "#6e7681", "#30363d", "#e6edf3", "#9198a1", "#4493f8", "#1c3a5e", "#0d1117"},
}

const (
	titleSize            = 14.0
	sublabelSize         = 12.0
	edgeLabelSize        = 11.5
	groupSize            = 12.0
	groupTracking        = 0.06
	nodeStroke           = 1.25
	edgeStroke           = 1.5
	groupStroke          = 1.0
	pulseStroke          = 2.5
	nodeRadius           = 10.0
	groupRadius          = 14.0
	edgeClearance        = 8.0
	parallelClearance    = 12.0
	pulseSeconds         = 0.8
	pulseDash            = 12.0
	pulseSpeed           = 280.0
	targetHighlightDelay = 0.1
	highlightFade        = 0.4
	highlightHold        = 0.8
	flowPause            = 1.0
	restSeconds          = 2.5
	easing               = "cubic-bezier(0.45,0,0.55,1)"
)

// The next pulse leaves once the node it starts from is fully lit.
const stepPause = targetHighlightDelay + highlightFade
