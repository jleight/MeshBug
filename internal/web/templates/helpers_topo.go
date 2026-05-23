package templates

import (
	"fmt"
	"math"
)

func humanDuration(secs int64) string {
	switch {
	case secs < 60:
		return fmt.Sprintf("%ds", secs)
	case secs < 3600:
		return fmt.Sprintf("%dm", secs/60)
	case secs < 86400:
		return fmt.Sprintf("%dh %dm", secs/3600, (secs%3600)/60)
	default:
		return fmt.Sprintf("%dd %dh", secs/86400, (secs%86400)/3600)
	}
}

func shortName(s string) string {
	if len(s) > 20 {
		return s[:18] + "…"
	}
	return s
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
	const (
		w      = 1000.0
		h      = 700.0
		rOuter = 280.0
		rInner = 140.0
	)

	cx, cy := w/2, h/2

	var obs, src []int
	for i, n := range nodes {
		if n.IsObserver {
			obs = append(obs, i)
		} else {
			src = append(src, i)
		}
	}

	for k, idx := range obs {
		theta := 2 * math.Pi * float64(k) / float64(max(1, len(obs)))
		nodes[idx].X = cx + rOuter*math.Cos(theta)
		nodes[idx].Y = cy + rOuter*math.Sin(theta)
	}

	for k, idx := range src {
		theta := 2 * math.Pi * float64(k) / float64(max(1, len(src)))
		nodes[idx].X = cx + rInner*math.Cos(theta)
		nodes[idx].Y = cy + rInner*math.Sin(theta)
	}

	return nodes
}
