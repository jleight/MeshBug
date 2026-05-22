// Package static embeds the vendored Tabler / Datastar assets into the binary.
package static

import (
	"embed"
	"io/fs"
)

//go:embed tabler/* datastar/*
var assets embed.FS

// FS returns the static asset tree rooted such that "/static/tabler/..." and
// "/static/datastar/..." paths resolve directly.
func FS() fs.FS { return assets }
