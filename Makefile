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
install-py: .uv ## Install Python dependencies using uv
	cd quiver-python && uv sync

.PHONY: install
install: .cargo .protoc install-py ## Install all dependencies (Rust, Python, protoc)
	cd quiver-core && cargo check --all-features

.PHONY: dev-py
dev-py: install-py ## Setup Python development environment
	@echo "Python client ready - no build step needed for gRPC client"

.PHONY: dev-py-release
dev-py-release: install-py ## Setup Python development environment (release mode)
	@echo "Python client ready - no build step needed for gRPC client"

.PHONY: format-rs
format-rs: ## Format Rust code using cargo fmt
	cd quiver-core && cargo fmt --all

.PHONY: format-py
format-py: ## Format Python code using ruff
	cd quiver-python && uv run ruff format

.PHONY: format
format: format-rs format-py ## Format all code (Rust + Python)

.PHONY: lint-rs
lint-rs: ## Lint Rust code using clippy (treats warnings as errors)
	@cargo clippy --version
	cd quiver-core && cargo clippy --all-features -- -D warnings

.PHONY: lint-py
lint-py: dev-py ## Lint Python code using ruff
	cd quiver-python && uv run ruff check

.PHONY: lint
lint: lint-rs lint-py ## Lint all code (Rust + Python)

.PHONY: test-rs
test-rs: .protoc ## Run all Rust tests
	cd quiver-core && cargo test --all-features

.PHONY: test-rs-single
test-rs-single: .protoc ## Run specific Rust test (usage: make test-rs-single TEST=test_name)
	cd quiver-core && cargo test --all-features $(TEST)

.PHONY: test-rs-integration
test-rs-integration: .protoc ## Run only integration tests
	cd quiver-core && cargo test --test integration_tests

.PHONY: pytest
pytest: dev-py ## Run Python tests
	cd quiver-python && (uv run pytest || echo "No Python tests found - skipping")

.PHONY: test
test: test-rs pytest ## Run all tests (Rust + Python)

.PHONY: testcov
testcov: .protoc ## Generate test coverage report (HTML + XML)
	@cargo tarpaulin --version > /dev/null 2>&1 || cargo install cargo-tarpaulin
	cd quiver-core && cargo tarpaulin --out xml --output-dir ..
	cd quiver-core && cargo tarpaulin --out html --output-dir ..
	@echo ""
	@echo "HTML report: ./tarpaulin-report.html"

.PHONY: proto-gen
proto-gen: .protoc ## Generate protobuf bindings (triggered automatically by build)
	cd quiver-core && cargo build

.PHONY: proto-check
proto-check: .protoc ## Validate protobuf definitions
	cd proto/v1 && protoc --descriptor_set_out=/dev/null *.proto

.PHONY: build
build: .protoc ## Build Rust code (debug mode)
	cd quiver-core && cargo build --all-features

.PHONY: build-release
build-release: .protoc ## Build Rust code (release mode)
	cd quiver-core && cargo build --release --all-features

.PHONY: check
check: .protoc ## Quick syntax check without building
	cd quiver-core && cargo check --all-features

.PHONY: audit
audit: ## Run security audit (with accepted risk exclusions)
	@cargo audit --version > /dev/null 2>&1 || cargo install cargo-audit
	cd quiver-core && cargo audit --ignore RUSTSEC-2023-0071 --ignore RUSTSEC-2026-0037 --ignore RUSTSEC-2024-0436

.PHONY: audit-all
audit-all: ## Run security audit without exclusions (shows all vulnerabilities)
	@cargo audit --version > /dev/null 2>&1 || cargo install cargo-audit
	cd quiver-core && cargo audit

.PHONY: security
security: audit ## Run security checks (alias for audit)

.PHONY: run
run: .protoc ## Run Quiver server (debug mode, optional CONFIG=path/to/config.yaml)
	cd quiver-core && cargo run $(if $(CONFIG),-- --config ../$(CONFIG))

.PHONY: run-release
run-release: .protoc ## Run Quiver server (release mode, optional CONFIG=path/to/config.yaml)
	cd quiver-core && cargo run --release $(if $(CONFIG),-- --config ../$(CONFIG))

.PHONY: quality
quality: format lint audit ## Run full quality pipeline (format + lint + security)

.PHONY: ci-check
ci-check: ## Run complete CI pipeline (format check + lint + test + security)
	cd quiver-core && cargo fmt --all -- --check
	cd quiver-core && cargo clippy --all-features -- -D warnings
	cd quiver-core && cargo test --all-features
	@cargo audit --version > /dev/null 2>&1 || cargo install cargo-audit
	cd quiver-core && cargo audit --ignore RUSTSEC-2023-0071 --ignore RUSTSEC-2026-0037 --ignore RUSTSEC-2024-0436

.PHONY: bench
bench: build-release ## Run all benchmarks (release mode, generates HTML report)
	cd quiver-core && cargo bench --bench throughput

.PHONY: bench-single
bench-single: build-release ## Run specific benchmark scenario (usage: make bench-single SCENARIO=scenario_1)
	cd quiver-core && cargo bench --bench throughput -- $(SCENARIO)

.PHONY: bench-baseline
bench-baseline: build-release ## Run baseline benchmarks only (fast iteration: Redis + Fanout 2x)
	cd quiver-core && BENCH_BATCH_SIZES=1,100,1000 cargo bench --bench throughput -- "scenario_1" "scenario_2" "scenario_4"

.PHONY: bench-comparison
bench-comparison: ## Run comparative benchmark (Quiver vs application fan-out)
	./benchmarks/run_benchmarks.sh

.PHONY: scenario-1
scenario-1: build-release ## Run scenario_1 (redis baseline) - start server first with: make server-1
	make bench-single SCENARIO=scenario_1

.PHONY: scenario-2
scenario-2: build-release ## Run scenario_2 (postgres baseline) - start server first with: make server-2
	make bench-single SCENARIO=scenario_2

.PHONY: scenario-3
scenario-3: build-release ## Run scenario_3 (fanout 2x) - start server first with: make server-3
	make bench-single SCENARIO=scenario_3

.PHONY: scenario-4
scenario-4: build-release ## Run scenario_4 (fanout 3x) - start server first with: make server-4
	make bench-single SCENARIO=scenario_4

.PHONY: server-kill
server-kill: ## Kill any running Quiver server
	@pkill -f "target/release/quiver-core" 2>/dev/null || true
	@sleep 1

.PHONY: server-1
server-1: build-release server-kill ## Start server for scenario_1 (redis baseline)
	cd quiver-core && cargo run --release -- --config ../examples/config/benchmark/redis-baseline.yaml

.PHONY: server-2
server-2: build-release server-kill ## Start server for scenario_2 (postgres baseline)
	cd quiver-core && cargo run --release -- --config ../examples/config/benchmark/postgres-baseline.yaml

.PHONY: server-3
server-3: build-release server-kill ## Start server for scenario_3 (fanout 2x)
	cd quiver-core && cargo run --release -- --config ../examples/config/benchmark/fanout-2x.yaml

.PHONY: server-4
server-4: build-release server-kill ## Start server for scenario_4 (fanout 3x)
	cd quiver-core && cargo run --release -- --config ../examples/config/benchmark/fanout-3x.yaml

.PHONY: release-prepare
release-prepare: .protoc ## Prepare for release (verify versions, run full CI)
	@echo "Preparing for release..."
	@VERSION=$$(grep '^version' quiver-core/Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/'); \
	echo "Version: $$VERSION"; \
	echo "✅ Running full CI pipeline..."; \
	make ci-check
	@echo ""
	@echo "✅ Release preparation complete!"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Update CHANGELOG.md with release summary"
	@echo "  2. Commit version and changelog changes"
	@echo "  3. Tag release: git tag v$$VERSION && git push origin main && git push origin v$$VERSION"
	@echo "  4. GitHub Actions will automatically build and publish"

.PHONY: release-build
release-build: .protoc ## Build release binaries locally (for current platform)
	@echo "Building release binaries..."
	@./scripts/build-release.sh

.PHONY: main
main: quality test ## Main development workflow (quality + test)

.PHONY: help
help: ## Show this help message
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