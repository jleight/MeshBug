package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/jleight/meshbug/internal/config"
	"github.com/jleight/meshbug/internal/store"
)

// runMigrate is a standalone migration runner used by the Helm
// pre-install / pre-upgrade hook (and available locally for `mise run
// migrate-up`). With no argument, applies every scope.
//
//	meshbug migrate            apply all scopes (common, ingest, project)
//	meshbug migrate all        same as above
//	meshbug migrate common     apply just the common scope
//	meshbug migrate ingest     apply just the ingest scope
//	meshbug migrate project    apply just the project scope
func runMigrate(args []string) {
	cfg, err := config.LoadWeb() // we only need DATABASE_URL + LOG_LEVEL
	if err != nil {
		fail("config", err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	scopes := store.AllScopes
	if len(args) > 0 {
		switch args[0] {
		case "all", "":
			scopes = store.AllScopes
		case "common":
			scopes = []store.Scope{store.ScopeCommon}
		case "ingest":
			scopes = []store.Scope{store.ScopeCommon, store.ScopeIngest}
		case "project":
			scopes = []store.Scope{store.ScopeCommon, store.ScopeProject}
		default:
			fmt.Fprintf(os.Stderr, "unknown scope %q. Valid: all, common, ingest, project\n", args[0])
			os.Exit(2)
		}
	}

	log.Info("applying migrations", "scopes", scopes)
	if err := store.RunMigrations(cfg.DatabaseURL, scopes...); err != nil {
		fail("migrate", err)
	}
	log.Info("migrations applied")
}
