# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26.5-bookworm AS build-base
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY gen ./gen
COPY internal ./internal

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

FROM build-base AS build-orchestrator
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath -ldflags="-s -w" -o /out/orchestrator ./cmd/orchestrator

FROM build-base AS build-worker
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/worker ./cmd/worker

FROM build-base AS build-target
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath -ldflags="-s -w" -o /out/target ./cmd/target

FROM build-base AS build-healthcheck
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath -ldflags="-s -w" -o /out/healthcheck ./cmd/healthcheck

FROM gcr.io/distroless/static-debian12:nonroot AS orchestrator
ARG VERSION=dev
ARG VCS_REF=unknown
ARG SOURCE_URL=https://github.com/marcuslin123/load-tester
LABEL org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.title="load-tester-orchestrator"
COPY --from=build-orchestrator /out/orchestrator /app/orchestrator
COPY --from=build-healthcheck /out/healthcheck /app/healthcheck
EXPOSE 9090 9091
ENTRYPOINT ["/app/orchestrator"]

FROM gcr.io/distroless/static-debian12:nonroot AS worker
ARG VERSION=dev
ARG VCS_REF=unknown
ARG SOURCE_URL=https://github.com/marcuslin123/load-tester
LABEL org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.title="load-tester-worker"
COPY --from=build-worker /out/worker /app/worker
ENTRYPOINT ["/app/worker"]

FROM gcr.io/distroless/static-debian12:nonroot AS target
ARG VERSION=dev
ARG VCS_REF=unknown
ARG SOURCE_URL=https://github.com/marcuslin123/load-tester
LABEL org.opencontainers.image.source="${SOURCE_URL}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.title="load-tester-target"
COPY --from=build-target /out/target /app/target
COPY --from=build-healthcheck /out/healthcheck /app/healthcheck
EXPOSE 8080
ENTRYPOINT ["/app/target"]
