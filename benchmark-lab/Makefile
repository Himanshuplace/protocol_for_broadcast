# Benchmark Lab Makefile
# Targets: build, test, bench, lint, proto-gen, cert-gen, docker-build, clean
#
# Environment variables:
#   PROTOCOLS   comma-separated list of protocols to benchmark (default: all)
#   SCENARIO    benchmark scenario: A|B|C|D|E (default: A)
#   DURATION    benchmark duration (default: 60s)
#   MSG_SIZE    message size in bytes (default: 1024)
#   GOAMD64     AMD64 feature level (default: v3, requires AVX2 — Intel Haswell+ / AMD Ryzen 1st gen+)

GOAMD64 ?= v3
GOFLAGS := -trimpath
LDFLAGS := -s -w
BINARY_DIR := bin
MODULE := github.com/himanshuplace/protocol_for_broadcast

# All server/client binary targets
BINARIES := \
	benchmark-runner \
	result-collector \
	udp-server udp-client \
	tcp-server tcp-client \
	http1-server http1-client \
	http2-server http2-client \
	http3-server http3-client \
	websocket-server websocket-client \
	sse-server sse-client \
	webtransport-server webtransport-client \
	webrtc-server webrtc-client

.PHONY: all build test bench lint proto-gen cert-gen docker-build clean run-smoke

all: proto-gen build test

## build: Compile all binaries into ./bin/
build:
	@mkdir -p $(BINARY_DIR)
	@for bin in $(BINARIES); do \
		echo "  BUILD $$bin"; \
		GOAMD64=$(GOAMD64) go build $(GOFLAGS) -ldflags="$(LDFLAGS)" \
			-o $(BINARY_DIR)/$$bin ./cmd/$$bin; \
	done

## test: Run all unit tests with race detector
test:
	GOAMD64=$(GOAMD64) go test -race -count=1 -timeout=120s ./...

## bench: Run Go benchmark tests for all packages
bench:
	GOAMD64=$(GOAMD64) go test -bench=. -benchmem -benchtime=5s -count=3 \
		-run='^$$' ./...

## bench-broadcast: Run only broadcast strategy benchmarks
bench-broadcast:
	GOAMD64=$(GOAMD64) go test -bench=BenchmarkBroadcast -benchmem -benchtime=10s \
		-count=5 -run='^$$' ./internal/broadcast/

## lint: Run golangci-lint
lint:
	golangci-lint run --timeout=5m ./...

## vet: Run go vet
vet:
	go vet ./...

## proto-gen: Regenerate protobuf code (requires buf)
proto-gen:
	buf generate

## cert-gen: Generate self-signed TLS certificates for local testing
cert-gen:
	@mkdir -p certs
	go run ./cmd/benchmark-runner cert-gen --dir certs

## docker-build: Build all Docker images
docker-build:
	@for bin in $(BINARIES); do \
		echo "  DOCKER BUILD $$bin"; \
		docker build \
			--build-arg BINARY=$$bin \
			--build-arg GOAMD64=$(GOAMD64) \
			-t benchmark-lab/$$bin:latest \
			-f docker/Dockerfile .; \
	done

## docker-up: Start the full benchmark stack
docker-up:
	docker compose -f docker/docker-compose.yml up -d

## docker-down: Stop the benchmark stack
docker-down:
	docker compose -f docker/docker-compose.yml down

## run-smoke: Quick smoke test (scenario A, UDP+TCP+WebSocket, 10s)
run-smoke:
	@mkdir -p results
	./$(BINARY_DIR)/benchmark-runner run \
		--protocols udp,tcp,websocket-gorilla \
		--scenario A \
		--msg-size 1024 \
		--duration 10s \
		--output results/smoke.json

## run-full: Full benchmark suite (all protocols, scenarios A-D)
run-full:
	@mkdir -p results
	GOAMD64=$(GOAMD64) ./$(BINARY_DIR)/benchmark-runner run \
		--protocols all \
		--scenario A,B,C,D \
		--msg-size 64,1024,65536 \
		--duration 60s \
		--output results/full-$(shell date +%Y%m%d-%H%M%S).json

## profile: Run with CPU+memory profiling enabled
profile:
	@mkdir -p results/profiles
	./$(BINARY_DIR)/benchmark-runner run \
		--protocols $(PROTOCOLS) \
		--scenario $(SCENARIO) \
		--msg-size $(MSG_SIZE) \
		--duration $(DURATION) \
		--cpuprofile results/profiles/ \
		--memprofile results/profiles/

## clean: Remove build artifacts
clean:
	rm -rf $(BINARY_DIR) results/smoke.json
	go clean -testcache

## tidy: Update go.sum
tidy:
	go mod tidy
