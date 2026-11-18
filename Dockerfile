# Pin the build image to the Go release containing the reachable standard
# library security fixes. A digest prevents a later mutable tag update from
# silently changing the release candidate build.
FROM golang:1.26.11-alpine@sha256:ecb67f987aa1a57794c91d19ade7ae0209f2f0f5496aaef9e6ae85ddf92d02ab AS build

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    for target in evalfrog control-plane worker-builtin worker-sandbox; do \
      CGO_ENABLED=0 go build -trimpath \
        -ldflags "-s -w -X github.com/uu999/evalfrog/internal/platform/buildinfo.Version=${VERSION} -X github.com/uu999/evalfrog/internal/platform/buildinfo.Commit=${COMMIT} -X github.com/uu999/evalfrog/internal/platform/buildinfo.Date=${BUILD_DATE}" \
        -o "/out/${target}" "./cmd/${target}"; \
    done

# CI runs integration tests from the authority network, which deliberately
# has no egress. Unlike a BuildKit cache mount, this module cache is part of
# the exported image so `go test` cannot fall back to proxy.golang.org.
FROM build AS integration-test-runner
RUN go mod download

FROM scratch AS runtime-base
WORKDIR /app
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY configs /app/configs
COPY migrations /app/migrations
USER 65532:65532

FROM runtime-base AS evalfrog
COPY --from=build /out/evalfrog /usr/local/bin/evalfrog
ENTRYPOINT ["/usr/local/bin/evalfrog"]

FROM runtime-base AS control-plane
COPY --from=build /out/control-plane /usr/local/bin/evalfrog
ENTRYPOINT ["/usr/local/bin/evalfrog"]

FROM runtime-base AS worker-builtin
COPY --from=build /out/worker-builtin /usr/local/bin/evalfrog
ENTRYPOINT ["/usr/local/bin/evalfrog"]

FROM runtime-base AS worker-sandbox
COPY --from=build /out/worker-sandbox /usr/local/bin/evalfrog
ENTRYPOINT ["/usr/local/bin/evalfrog"]

# The local Sandbox Runtime Controller is deliberately a different image from
# worker-sandbox. It owns Docker only on a dedicated local sandbox node; the
# sandbox Worker has no Docker CLI or socket. Production substitutes its
# hardened runtime/controller implementation behind the same private protocol.
FROM alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc AS sandbox-runtime
RUN apk add --no-cache ca-certificates docker-cli
WORKDIR /app
COPY --from=build /out/worker-sandbox /usr/local/bin/evalfrog
COPY configs /app/configs
ENTRYPOINT ["/usr/local/bin/evalfrog", "--sandbox-runtime"]
