package web

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"io/fs"
	"net/http"
	"strings"

	xdraw "golang.org/x/image/draw"
)

// iconSpec describes a single resized icon variant served from memory.
type iconSpec struct {
	name string
	size int
}

// iconSpecs is the set of variants generated from the source logo at
// startup. Names match the HTML <link> references and the routes that
// serve them.
var iconSpecs = []iconSpec{
	{"favicon-16.png", 16},
	{"favicon-32.png", 32},
	{"favicon-48.png", 48},
	{"apple-touch-icon.png", 180},
	{"android-chrome-192.png", 192},
	{"android-chrome-512.png", 512},
	{"logo-32.png", 32},
	{"logo-64.png", 64},
}

// IconSet holds the generated PNG bytes keyed by filename, plus the
// favicon bytes used for /favicon.ico requests.
type IconSet struct {
	files       map[string][]byte
	faviconICO  []byte
}

// BuildIcons reads the source logo from the embedded static FS and
// produces every variant in iconSpecs by aspect-preserving resize onto
// a transparent square canvas.
func BuildIcons(staticFS fs.FS) (*IconSet, error) {
	src, err := loadSourceLogo(staticFS)
	if err != nil {
		return nil, err
	}

	out := &IconSet{
		files: make(map[string][]byte, len(iconSpecs)),
	}

	for _, spec := range iconSpecs {
		buf, err := renderSquarePNG(src, spec.size)
		if err != nil {
			return nil, fmt.Errorf("render %s: %w", spec.name, err)
		}

		out.files[spec.name] = buf
	}

	out.faviconICO = out.files["favicon-32.png"]

	return out, nil
}

func loadSourceLogo(staticFS fs.FS) (image.Image, error) {
	f, err := staticFS.Open("meshbug/logo.png")
	if err != nil {
		return nil, fmt.Errorf("open source logo: %w", err)
	}
	defer func(f fs.File) {
		_ = f.Close()
	}(f)

	img, err := png.Decode(f)
	if err != nil {
		return nil, fmt.Errorf("decode source logo: %w", err)
	}

	return img, nil
}

// renderSquarePNG scales src into a size×size transparent canvas while
// preserving the source aspect ratio, then encodes the result as PNG.
func renderSquarePNG(src image.Image, size int) ([]byte, error) {
	canvas := image.NewNRGBA(image.Rect(0, 0, size, size))

	draw.Draw(
		canvas,
		canvas.Bounds(),
		image.NewUniform(color.NRGBA{}),
		image.Point{},
		draw.Src,
	)

	srcW := src.Bounds().Dx()
	srcH := src.Bounds().Dy()

	scale := float64(size) / float64(srcW)
	if h := float64(size) / float64(srcH); h < scale {
		scale = h
	}

	dstW := int(float64(srcW) * scale)
	dstH := int(float64(srcH) * scale)

	offX := (size - dstW) / 2
	offY := (size - dstH) / 2

	dst := image.Rect(offX, offY, offX+dstW, offY+dstH)

	xdraw.CatmullRom.Scale(
		canvas,
		dst,
		src,
		src.Bounds(),
		xdraw.Over,
		nil,
	)

	var buf bytes.Buffer

	err := png.Encode(&buf, canvas)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// handler returns an http.Handler that serves the generated icons at
// /static/meshbug/<name>.
func (s *IconSet) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/static/meshbug/")

		body, ok := s.files[name]
		if !ok {
			http.NotFound(w, r)
			return
		}

		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "public, max-age=86400")
		_, _ = w.Write(body)
	})
}

// favicon serves /favicon.ico with the 32px PNG.
func (s *IconSet) favicon(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "image/x-icon")
	w.Header().Set("Cache-Control", "public, max-age=86400")
	_, _ = w.Write(s.faviconICO)
}
