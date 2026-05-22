# Contributing to MeshBug

Thanks for your interest in MeshBug. A few short notes before you send a PR.

## License

MeshBug is distributed under the [Elastic License 2.0](./LICENSE). By using
the software you agree to those terms. In particular, you may not provide
MeshBug to third parties as a hosted or managed service that gives users
access to a substantial set of MeshBug's features.

## Contributor License Agreement

All contributions are accepted only under the terms of the
[MeshBug CLA](./CLA.md). The CLA grants the project owner the rights needed
to keep distributing MeshBug under ELv2 today and to relicense it if the
project's needs change in the future.

**You only need to sign once.** On your first pull request, a bot will
comment asking you to reply with this exact line:

> I have read the MeshBug CLA and agree to its terms for this and all future
> contributions to the Project.

The bot records your signature in `signatures/v1/cla.json` on the
`cla-signatures` branch of this repository. Every subsequent PR from the
same GitHub account is auto-recognized — no per-commit sign-off needed and
no further action required.

If you are contributing on behalf of an employer, also add to the PR
description:

> I am authorized to sign on behalf of *Company Name*, and this contribution
> is made on its behalf.

## Development quickstart

```sh
mise install                 # toolchain (go, templ, golang-migrate, helm, ...)
mise run init-local          # writes .mise/config.local.toml — fill in creds
mise run pg-up               # local Postgres in Docker
mise run migrate-up
mise run run                 # connects to MQTT brokers and starts the web UI
```

`mise run test`, `mise run lint`, `mise run build`, `mise run docker`, and
`mise run helm-{lint,template}` cover the rest. See `.mise/config.toml` for
the full task list.

## Code conventions

- Standard `gofmt`; `mise run lint` runs `golangci-lint`.
- SQL lives in `internal/store/queries.go` and migrations in
  `internal/store/migrations/`.
- Templates are `*.templ` files; run `mise run generate` after editing them.
- New features should come with focused tests, especially around the
  MeshCore decoder and ingest parsing.

## Reporting issues

Open a GitHub issue with:

- What you saw (logs, packet captures, screenshots).
- What you expected.
- The MeshBug version (`git rev-parse HEAD` is fine).
- Anything specific about your mesh setup that might matter.

## Security issues

Please report potential security issues privately by emailing the project
owner rather than filing a public issue.
