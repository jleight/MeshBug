package templates

import (
	"fmt"
	"strings"
)

func humanDuration(secs int64) string {
	if secs < 60 {
		return fmt.Sprintf("%ds", secs)
	}
	if secs < 3600 {
		return fmt.Sprintf("%dm", secs/60)
	}
	if secs < 86400 {
		return fmt.Sprintf("%dh %dm", secs/3600, (secs%3600)/60)
	}
	return fmt.Sprintf("%dd %dh", secs/86400, (secs%86400)/3600)
}

func shortName(s string) string {
	if len(s) > 20 {
		return s[:18] + "…"
	}
	return s
}

func findNode(nodes []TopoNode, id string) *TopoNode {
	for i := range nodes {
		if nodes[i].ID == id {
			return &nodes[i]
		}
	}
	return nil
}

func edgeColor(rssi float64) string {
	switch {
	case rssi >= -70:
		return "#2fb344"
	case rssi >= -90:
		return "#f59f00"
	default:
		return "#d6336c"
	}
}

func edgeWidth(packets int) float64 {
	if packets <= 1 {
		return 0.6
	}
	w := 0.6 + 0.3*float64(packets-1)
	if w > 5 {
		w = 5
	}
	return w
}

// LayoutTopology positions nodes in a simple circle layout: observers on the
// outer ring, sources on an inner ring. Good enough until we want a real
// force-directed pass.
func LayoutTopology(nodes []TopoNode) []TopoNode {
	const w, h = 1000.0, 700.0
	cx, cy := w/2, h/2
	rOuter := 280.0
	rInner := 140.0
	var obs, src []int
	for i, n := range nodes {
		if n.IsObserver {
			obs = append(obs, i)
		} else {
			src = append(src, i)
		}
	}
	for k, idx := range obs {
		theta := 2 * 3.14159 * float64(k) / float64(maxInt(1, len(obs)))
		nodes[idx].X = cx + rOuter*cosf(theta)
		nodes[idx].Y = cy + rOuter*sinf(theta)
	}
	for k, idx := range src {
		theta := 2 * 3.14159 * float64(k) / float64(maxInt(1, len(src)))
		nodes[idx].X = cx + rInner*cosf(theta)
		nodes[idx].Y = cy + rInner*sinf(theta)
	}
	return nodes
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// tiny inline trig to avoid an import cycle in test setup
func cosf(t float64) float64 { return cosTaylor(t) }
func sinf(t float64) float64 { return sinTaylor(t) }

func sinTaylor(x float64) float64 {
	// reduce to [-pi, pi]
	for x > 3.14159265 {
		x -= 2 * 3.14159265
	}
	for x < -3.14159265 {
		x += 2 * 3.14159265
	}
	x2 := x * x
	return x * (1 - x2/6*(1-x2/20*(1-x2/42*(1-x2/72))))
}
func cosTaylor(x float64) float64 { return sinTaylor(x + 1.57079632) }

func init() {
	// silence unused import warning when build tags hide consumers
	_ = strings.Builder{}
}
