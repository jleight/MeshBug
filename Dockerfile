# syntax=docker/dockerfile:1.7

# Builder always runs on the host arch (BUILDPLATFORM), and Go cross-compiles
# to TARGETARCH. This avoids running every build step under QEMU emulation,
# which is 5-10x slower than native — a multi-arch build that took ~6 minutes
# under emulation drops to ~1-2 minutes this way.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

ARG TARGETOS
ARG TARGETARCH
ARG TABLER_VERSION=
ARG TABLER_ICONS_VERSION=
ARG DATASTAR_VERSION=

WORKDIR /src
RUN apk add --no-cache git ca-certificates curl bash

# Modules first so a code-only change doesn't bust the dependency cache.
COPY go.mod go.sum ./
RUN go mod download

# Fetch vendored web assets via the same script the local mise task uses.
COPY .mise/tasks/fetch-assets .mise/tasks/fetch-assets
RUN TABLER_VERSION="${TABLER_VERSION}" \
    TABLER_ICONS_VERSION="${TABLER_ICONS_VERSION}" \
    DATASTAR_VERSION="${DATASTAR_VERSION}" \
    bash .mise/tasks/fetch-assets

# templ runs on the build host (BUILDPLATFORM) — fast, no QEMU.
RUN go install github.com/a-h/templ/cmd/templ@v0.3.1020

COPY . .

RUN templ generate \
 && CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-s -w" -o /out/meshbug ./cmd/meshbug

# Final stage is a static, statically-linked image. distroless:static is a
# manifest list — buildx picks the right arch per target. The COPY runs on
# the host, so no QEMU is required for this stage either.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/meshbug /meshbug
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/meshbug"]
