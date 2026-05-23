// MeshBug — MeshCore mesh health dashboard.
//
// Copyright (c) 2026 Jonathon Leight.
// Licensed under the Elastic License 2.0; see the LICENSE file in the repo
// root.
//
// One binary, three subcommands forming an event-sourcing pipeline:
//
//	meshbug ingest    subscribe to MQTT brokers, write every message verbatim
//	                  into the `raw_events` table. No parsing, no decoding.
//	meshbug project   read `raw_events`, decode + derive observers/packets/
//	                  rollups/anomalies. `--reset` rebuilds from scratch.
//	meshbug web       serve the dashboard UI, listen for pg_notify from the
//	                  projector, push SSE updates to connected browsers.
//
// Run them as independent Deployments. The only thing they share is the
// Postgres database (rows + LISTEN/NOTIFY traffic).

package main

import (
	"fmt"
	"log/slog"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	switch cmd {
	case "migrate":
		runMigrate(args)
	case "ingest":
		runIngest(args)
	case "project":
		runProject(args)
	case "web":
		runWeb(args)
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `meshbug — MeshCore mesh health dashboard

usage:
  meshbug migrate [all|common|ingest|project]
                          Apply database migrations. Used by the Helm
                          pre-install/pre-upgrade hook and mise run migrate-up.
  meshbug ingest          Capture MQTT messages into raw_events.
  meshbug project         Derive state from raw_events into the read tables.
  meshbug project --reset Truncate every derived table and rebuild from raw_events.
  meshbug web             Serve the dashboard UI (reads only).

Configuration is via environment variables. See README for the full list.
`)
}

func fail(stage string, err error) {
	slog.Error("startup failed", "stage", stage, "err", err)
	os.Exit(1)
}
