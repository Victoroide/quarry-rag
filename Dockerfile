FROM golang:1.27.3-alpine3.23@sha256:5d5113301ffcac84e2bfb20c42648803fecaea6789c89bb9b66884e18cbb30a7 AS base
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
FROM base AS builder
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/server ./cmd/server
RUN CGO_ENABLED=0 sh .github/scripts/third-party-licenses.sh ./cmd/server /licenses

# ── final runtime image (minimal) ───────────────────────
FROM alpine:3.24.3@sha256:be2656061b536c1883c4034790e21d0a09c5aaaf51968b89368a3c0371ea2c9a AS runtime
RUN apk add --no-cache ca-certificates tzdata && addgroup -S -g 10001 app && adduser -S -u 10001 -G app app
WORKDIR /app
COPY --from=builder /bin/server /app/server
COPY --from=builder /licenses /usr/share/licenses/quarry-rag
COPY LICENSE /usr/share/licenses/quarry-rag/LICENSE
USER 10001:10001
EXPOSE 8081
ENTRYPOINT ["/app/server"]
