package web

import (
	"context"
	"fmt"
)

func sprintf(f string, args ...any) string { return fmt.Sprintf(f, args...) }

// Re-export for handler files.
var _ = context.Background
