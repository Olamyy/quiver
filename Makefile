.DEFAULT_GOAL := main

.PHONY: .cargo
.cargo: 
	@cargo --version || echo 'Please install cargo: https://github.com/rust-lang/cargo'

.PHONY: .uv
.uv: 
	@uv --version || echo 'Please install uv: https://docs.astral.sh/uv/getting-started/installation/'

.PHONY: .protoc
.protoc: 
	@protoc --version || echo 'Please install protoc: https://grpc.io/docs/protoc-installation/'

.PHONY: install-py
install-py: .uv 
	cd quiver-python && uv sync

.PHONY: install
install: .cargo .protoc install-py 
	cd quiver-core && cargo check --all-features

.PHONY: dev-py
dev-py: install-py 
	@echo "Python client ready - no build step needed for gRPC client"

.PHONY: dev-py-release
dev-py-release: install-py 
	@echo "Python client ready - no build step needed for gRPC client"

.PHONY: format-rs
format-rs: 
	cd quiver-core && cargo fmt --all

.PHONY: format-py
format-py: 
	cd quiver-python && uv run ruff format

.PHONY: format
format: format-rs format-py 

.PHONY: lint-rs
lint-rs: 
	@cargo clippy --version
	cd quiver-core && cargo clippy --all-features -- -D warnings

.PHONY: lint-py
lint-py: dev-py 
	cd quiver-python && uv run ruff check

.PHONY: lint
lint: lint-rs lint-py 

.PHONY: test-rs
test-rs: .protoc 
	cd quiver-core && cargo test --all-features

.PHONY: test-rs-single
test-rs-single: .protoc 
	cd quiver-core && cargo test --all-features $(TEST)

.PHONY: test-rs-integration
test-rs-integration: .protoc 
	cd quiver-core && cargo test --test integration_tests

.PHONY: pytest
pytest: dev-py 
	cd quiver-python && (uv run pytest || echo "No Python tests found - skipping")

.PHONY: test
test: test-rs pytest 

.PHONY: testcov
testcov: .protoc 
	@cargo tarpaulin --version > /dev/null 2>&1 || cargo install cargo-tarpaulin
	cd quiver-core && cargo tarpaulin --out xml --output-dir ..
	cd quiver-core && cargo tarpaulin --out html --output-dir ..
	@echo ""
	@echo "HTML report: ./tarpaulin-report.html"

.PHONY: proto-gen
proto-gen: .protoc 
	cd quiver-core && cargo build

.PHONY: proto-check
proto-check: .protoc 
	cd proto/v1 && protoc --descriptor_set_out=/dev/null *.proto

.PHONY: build
build: .protoc 
	cd quiver-core && cargo build --all-features

.PHONY: build-release
build-release: .protoc 
	cd quiver-core && cargo build --release --all-features

.PHONY: check
check: .protoc 
	cd quiver-core && cargo check --all-features

.PHONY: run
run: .protoc 
	cd quiver-core && cargo run

.PHONY: run-release
run-release: .protoc 
	cd quiver-core && cargo run --release

.PHONY: quality
quality: format lint 

.PHONY: ci-check
ci-check: 
	cd quiver-core && cargo fmt --all -- --check
	cd quiver-core && cargo clippy --all-features -- -D warnings
	cd quiver-core && cargo test --all-features

.PHONY: redis-start
redis-start: 
	@command -v redis-server > /dev/null || echo 'Please install Redis: https://redis.io/download'
	redis-server --port 6379 --daemonize yes

.PHONY: redis-stop
redis-stop: 
	redis-cli shutdown || echo 'Redis server not running or already stopped'

.PHONY: redis-seed
redis-seed: 
	python3 seed_redis.py

# Benchmark commands
.PHONY: bench
bench: ## Run complete benchmark suite proving Quiver's ML inference value
	cd benchmarks && cargo run --bin benchmark

.PHONY: bench-quick
bench-quick: ## Run quick benchmark verification
	@echo "Running quick benchmark verification..."
	cd benchmarks && cargo run --bin benchmark &
	@sleep 5
	@pkill -f "cargo run --bin benchmark" 2>/dev/null || true
	@echo "Quick benchmark verification completed"

.PHONY: bench-criterion
bench-criterion: ## Run precise Criterion-based benchmarks
	cd benchmarks && cargo bench

.PHONY: bench-report
bench-report: ## Generate and display benchmark reports
	cd benchmarks && cargo run --bin benchmark && echo "Check reports/ directory for detailed results"

.PHONY: main
main: quality test 

.PHONY: help
help: 
	@echo "Usage: make [recipe]"
	@echo "Recipes:"
	@awk '/^[a-zA-Z0-9_-]+:.*?##/ { \
	    helpMessage = match($$0, /## (.*)/); \
	        if (helpMessage) { \
	            recipe = $$1; \
	            sub(/:/, "", recipe); \
	            printf "  \033[36mmake %-20s\033[0m %s\n", recipe, substr($$0, RSTART + 3, RLENGTH); \
	    } \
	}' $(MAKEFILE_LIST)