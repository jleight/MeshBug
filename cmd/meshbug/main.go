// MeshBug — MeshCore mesh health dashboard.
//
// Copyright (c) 2026 Jonathon Leight.
// Licensed under the Elastic License 2.0; see the LICENSE file in the repo
// root.
//
// One binary, two subcommands:
//
//	meshbug ingest   — subscribe to MQTT brokers, write to Postgres,
//	                   run rollup + anomaly workers, fire pg_notify on writes.
//	meshbug web      — serve the dashboard UI, listen for pg_notify, push
//	                   SSE updates to connected browsers.
//
// They share the same /internal packages and the same Postgres database. Run
// them as separate Deployments (ingest in your cluster, web wherever) and the
// only thing they exchange is rows + LISTEN/NOTIFY traffic.

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
	case "ingest":
		runIngest(args)
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
  meshbug ingest    Run the MQTT ingest service (writes to Postgres).
  meshbug web       Run the web UI / SSE service (reads from Postgres).

Configuration is via environment variables. See README for the full list.
`)
}

func fail(stage string, err error) {
	slog.Error("startup failed", "stage", stage, "err", err)
	os.Exit(1)
}
