package templates

import (
	"fmt"
	"strings"
	"time"
)

func sparklinePoints(values []float64, width, height int) string {
	if len(values) == 0 {
		return ""
	}

	mn, mx := values[0], values[0]
	for _, v := range values[1:] {
		if v < mn {
			mn = v
		}
		if v > mx {
			mx = v
		}
	}

	rng := mx - mn
	if rng == 0 {
		rng = 1
	}

	var b strings.Builder

	for i, v := range values {
		x := float64(i) * float64(width-2) / float64(max(1, len(values)-1))
		y := float64(height-2) - (v-mn)/rng*float64(height-4) + 1

		if i > 0 {
			b.WriteByte(' ')
		}

		fmt.Fprintf(&b, "%.1f,%.1f", x+1, y)
	}

	return b.String()
}

func rssiBadgeClass(rssi int) string {
	switch {
	case rssi >= -70:
		return "mb-rssi-good"
	case rssi >= -90:
		return "mb-rssi-ok"
	default:
		return "mb-rssi-bad"
	}
}

func humanAgo(t time.Time) string {
	d := time.Since(t)
	if d < 0 {
		d = 0
	}

	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))

	case d < time.Hour:
		s := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm%ds ago", int(d.Minutes()), s)

	case d < 24*time.Hour:
		return fmt.Sprintf("%dh%dm ago", int(d.Hours()), int(d.Minutes())%60)

	default:
		return fmt.Sprintf("%dd%dh ago", int(d.Hours())/24, int(d.Hours())%24)
	}
}

// HexShort returns a short hex prefix for a byte hash (8 chars).
func HexShort(b []byte) string {
	if len(b) == 0 {
		return "—"
	}

	const hex = "0123456789abcdef"

	n := len(b)
	if n > 4 {
		n = 4
	}

	out := make([]byte, n*2)
	for i := 0; i < n; i++ {
		out[i*2] = hex[b[i]>>4]
		out[i*2+1] = hex[b[i]&0x0F]
	}

	return string(out)
}

// HexFull returns the full lowercase hex of a byte slice.
func HexFull(b []byte) string {
	const hex = "0123456789abcdef"

	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2] = hex[c>>4]
		out[i*2+1] = hex[c&0x0F]
	}

	return string(out)
}
