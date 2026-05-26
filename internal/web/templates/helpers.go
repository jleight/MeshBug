package templates

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/a-h/templ"
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

// nodesPageURL builds a /nodes URL preserving filters but with the
// page number replaced.
func nodeDisplayName(n NodeRow) string {
	if n.Name != "" {
		return n.Name
	}

	return HexShort(n.PublicKey) + "…"
}

func nodeTypeLabel(t string) string {
	if t == "" {
		return "NONE"
	}

	return t
}

// nodeMapEmbedURL builds an OSM iframe URL centered on the node, with a
// marker at its lat/lon. The bbox is a fixed ~0.02 degree window so the
// initial zoom is roughly city-scale.
func nodeMapEmbedURL(n NodeRow) templ.SafeURL {
	const halfSpan = 0.01

	lat := float64(n.LatE6) / 1e6
	lon := float64(n.LonE6) / 1e6

	bbox := fmt.Sprintf(
		"%.6f,%.6f,%.6f,%.6f",
		lon-halfSpan,
		lat-halfSpan,
		lon+halfSpan,
		lat+halfSpan,
	)

	marker := fmt.Sprintf("%.6f,%.6f", lat, lon)

	return templ.SafeURL(
		"https://www.openstreetmap.org/export/embed.html?bbox=" + bbox + "&layer=mapnik&marker=" + marker,
	)
}

func nodesPageURL(d NodesPage, page int) string {
	return nodesURL(d.Sort, d.Dir, d.Filter, page, d.Size)
}

// nodesSortURL builds a /nodes URL that asks for sorting by `col`. If
// `col` is already the current sort, the direction is toggled.
func nodesSortURL(d NodesPage, col string) string {
	dir := "asc"
	if d.Sort == col && d.Dir == "asc" {
		dir = "desc"
	}

	return nodesURL(col, dir, d.Filter, 1, d.Size)
}

func nodesURL(
	sort string,
	dir string,
	filter NodeFilter,
	page int,
	size int,
) string {
	q := url.Values{}

	if filter.Q != "" {
		q.Set("q", filter.Q)
	}

	if filter.Type != "" {
		q.Set("type", filter.Type)
	}

	if sort != "" {
		q.Set("sort", sort)
	}

	if dir != "" {
		q.Set("dir", dir)
	}

	if page > 1 {
		q.Set("page", strconv.Itoa(page))
	}

	if size > 0 {
		q.Set("size", strconv.Itoa(size))
	}

	enc := q.Encode()
	if enc == "" {
		return "/nodes"
	}

	return "/nodes?" + enc
}

func nodesRangeStart(d NodesPage) int {
	if d.Total == 0 {
		return 0
	}

	return (d.Page-1)*d.Size + 1
}

func nodesRangeEnd(d NodesPage) int {
	end := d.Page * d.Size
	if end > d.Total {
		end = d.Total
	}

	return end
}

func nodesTotalPages(d NodesPage) int {
	if d.Size <= 0 || d.Total <= 0 {
		return 1
	}

	pages := (d.Total + d.Size - 1) / d.Size
	if pages < 1 {
		pages = 1
	}

	return pages
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
