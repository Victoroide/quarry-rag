.PHONY: build test lint vet ci image-check up down logs cleanup reconcile validate fuzz diagrams diagrams-check

FUZZTIME ?= 30s

# Run inside container — no host deps needed
build:
	docker build --target builder -t quarry-rag:build .

# Run static analysis on the host.
vet:
	go vet ./... ./.github/scripts/testreport

lint:
	golangci-lint run ./... ./.github/scripts/testreport

# Database-backed tests start containers unless the test services are configured; see docs/development.md.
test:
	go test -race -count=1 ./...

fuzz:
	FUZZTIME="$(FUZZTIME)" sh .github/scripts/fuzz.sh

# Run host quality gates before building the runtime image.
ci:
	$(MAKE) vet
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) diagrams-check
	docker build --target runtime -t quarry-rag:prod .

# Build the runtime image for both published platforms and check each of them.
image-check:
	set -e; dir=$$(mktemp -d); trap 'rm -rf "$$dir"' EXIT; \
	docker buildx build --target runtime --platform linux/amd64,linux/arm64 --output "type=oci,dest=$$dir/runtime-image.tar" .; \
	docker buildx build --target verify-runtime --build-context "runtime-output=$$dir" --output type=cacheonly --progress plain .

# Start the full stack
up:
	docker-compose up --build -d

down:
	docker-compose down

logs:
	docker-compose logs -f app

# Delete explicit document UUIDs: make cleanup IDS="<uuid> <uuid>".
cleanup:
	go run ./scripts/cleanup $(IDS)

reconcile:
	go run ./scripts/reconcile $(ARGS)

validate:
	go run ./scripts/validate/main.go

# Refresh the documentation images.
diagrams:
	go -C tools/diagrams run .

# Validate sources, geometry and reproducible output.
diagrams-check:
	go -C tools/diagrams test ./...
	go -C tools/diagrams run . -check
