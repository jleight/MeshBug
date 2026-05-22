FROM golang:1.26-alpine AS builder
WORKDIR /src
RUN apk add --no-cache git ca-certificates curl

# Optional overrides for asset versions. Defaults come from .mise/tasks/fetch-assets.
ARG TABLER_VERSION=
ARG TABLER_ICONS_VERSION=
ARG DATASTAR_VERSION=

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Fetch vendored web assets via the same script the local mise task uses.
# Versions are pinned inside the script; override at build time with --build-arg.
RUN apk add --no-cache bash \
 && TABLER_VERSION="${TABLER_VERSION:-}" \
    TABLER_ICONS_VERSION="${TABLER_ICONS_VERSION:-}" \
    DATASTAR_VERSION="${DATASTAR_VERSION:-}" \
    bash .mise/tasks/fetch-assets

RUN go install github.com/a-h/templ/cmd/templ@v0.3.1020 \
 && templ generate \
 && CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/meshbug ./cmd/meshbug

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/meshbug /meshbug
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/meshbug"]
