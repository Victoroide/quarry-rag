FROM golang:1.27.4-alpine3.23@sha256:f70850ef6ce341e8b6427f677049145d63c0b7ce002d4d7e3d464828b1b96dcd AS base
RUN apk add --no-cache gcc musl-dev git
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

# ── test stage ──────────────────────────────────────────
FROM base AS test
COPY . .
RUN go vet ./...
RUN CGO_ENABLED=1 go test -short ./... -count=1 -race

# ── lint stage ──────────────────────────────────────────
FROM base AS lint
RUN go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0
COPY . .
RUN golangci-lint run --config=.golangci.yml

# ── builder stage ───────────────────────────────────────
# Compiles on the build host for the requested target, so a foreign architecture
# is never emulated. The dependency walk uses the same target as the build.
FROM --platform=$BUILDPLATFORM golang:1.27.4-alpine3.23@sha256:f70850ef6ce341e8b6427f677049145d63c0b7ce002d4d7e3d464828b1b96dcd AS build-base
RUN apk add --no-cache git
ENV GOTOOLCHAIN=local
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

FROM build-base AS builder
ARG TARGETOS
ARG TARGETARCH
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /bin/server ./cmd/server
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH sh .github/scripts/third-party-licenses.sh ./cmd/server /licenses

# Derive the expected notices from the source and pinned toolchain in a
# separate build. The OCI archive supplies only the image being checked.
FROM build-base AS verify-runtime
COPY . .
RUN --mount=type=bind,from=runtime-output,target=/image \
    sh .github/scripts/image-check.sh /image/runtime-image.tar

# ── final runtime image (minimal) ───────────────────────
# Stays on the target platform so its packages and user are installed for it.
FROM alpine:3.24.4@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf AS runtime
RUN apk add --no-cache ca-certificates tzdata && addgroup -S -g 10001 app && adduser -S -u 10001 -G app app
WORKDIR /app
COPY --from=builder /bin/server /app/server
COPY --from=builder /licenses /usr/share/licenses/quarry-rag
COPY LICENSE /usr/share/licenses/quarry-rag/LICENSE
USER 10001:10001
EXPOSE 8081
ENTRYPOINT ["/app/server"]
