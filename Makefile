SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eo pipefail -c

CARGO ?= cargo
CARGO_PLAIN ?= $(filter-out --locked --frozen,$(CARGO))
CURL ?= curl
CARGO_HEATHER_VERSION := 0.3.0
CCM ?= ccm
CCM_CLUSTER ?= alternator-client-rust
CCM_IP_PREFIX ?= 127.0.0.
CCM_NODE ?= node1
CCM_SCYLLA_VERSION ?= release:2026.1.14
ALTERNATOR_TEST_ADDRESS ?= $(CCM_IP_PREFIX)1:8000
ALTERNATOR_READY_TIMEOUT ?= 60
RUSTFLAGS_CCM ?= --cfg ccm_tests
RUSTDOC_TOOLCHAIN ?= nightly-2026-06-23
RUSTC ?= rustc
SCYLLA_ALTERNATOR_HTTP_COMPRESSION ?= true
EXPECTED_RUST_HOST ?=
EXPECTED_LIB_TESTS ?= 230
EXPECTED_DOCTESTS ?= 14
CANDIDATE_CRATE ?=
PACKAGE_NAME ?= $(shell awk -F '"' '/^name = "/ { print $$2; exit }' Cargo.toml)
PACKAGE_VERSION ?= $(shell awk -F '"' '/^version = "/ { print $$2; exit }' Cargo.toml)
PACKAGE_INSPECT_SCRIPT ?= scripts/release/inspect-package.sh
CRATES_IO_SPARSE_INDEX ?= https://index.crates.io

define SCYLLA_START_COMMANDS
$(CCM) remove "$(CCM_CLUSTER)" >/dev/null 2>&1 || true
$(CCM) create "$(CCM_CLUSTER)" -n 1 -i "$(CCM_IP_PREFIX)" --scylla -v "$(CCM_SCYLLA_VERSION)"
$(CCM) "$(CCM_NODE)" updateconf \
	alternator_address:$(CCM_IP_PREFIX)1 \
	alternator_port:8000 \
	alternator_write_isolation:always \
	alternator_response_gzip_compression_level:6 \
	alternator_response_compression_threshold_in_bytes:1
$(CCM) start --wait-for-binary-proto --wait-other-notice
endef

define WAIT_FOR_ALTERNATOR_COMMANDS
echo "Waiting for Alternator to be ready..."
ready=false
started=$$SECONDS
deadline=$$((started + $(ALTERNATOR_READY_TIMEOUT)))
while (( SECONDS < deadline )); do
	if $(CURL) -sf --connect-timeout 1 --max-time 1 "http://$(ALTERNATOR_TEST_ADDRESS)/localnodes" >/dev/null 2>&1; then
		echo "Alternator is ready (waited $$((SECONDS - started))s)"
		ready=true
		break
	fi
	sleep 1
done
if [[ "$$ready" != true ]]; then
	echo "Timed out waiting for Alternator"
	exit 1
fi
endef

.PHONY: clean verify lint static-checks lint-docs lint-fix license-install license-check license-fix
.PHONY: compile compile-test build-release assert-rust-host test-unit test-lib test-doctests test-hermetic
.PHONY: test-dynamodb-compatibility test-native-certs test-downstream test-portable
.PHONY: test-integration test-server test-ccm test-all
.PHONY: security-checks semver-check package-dry-run package-inspect package-check
.PHONY: .prepare-ccm .prepare-environment-update-aio-max-nr
.PHONY: wait-for-alternator scylla-start scylla-stop scylla-kill scylla-rm
.PHONY: logs cqlsh

lint: license-check
	$(CARGO) fmt --all -- --check
	$(CARGO) check --locked --all-targets
	$(CARGO) clippy --locked --all-targets -- -D warnings
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --locked --no-deps

static-checks: lint

clean:
	$(CARGO) clean

verify: lint test-all

lint-docs:
	RUSTDOCFLAGS="-D warnings" $(CARGO) doc --locked --no-deps

lint-fix:
	$(CARGO) fmt --all

license-install:
	@command -v cargo-heather >/dev/null || $(CARGO) install cargo-heather --version $(CARGO_HEATHER_VERSION) --locked

license-check: license-install
	@set -euo pipefail; \
	if [[ -f Cargo.toml.orig ]]; then \
		normalized_manifest="$$(mktemp)"; \
		cp Cargo.toml "$$normalized_manifest"; \
		trap 'cp "$$normalized_manifest" Cargo.toml; rm -f "$$normalized_manifest"' EXIT; \
		cp Cargo.toml.orig Cargo.toml; \
		$(CARGO) heather; \
		cp "$$normalized_manifest" Cargo.toml; \
		rm -f "$$normalized_manifest"; \
		trap - EXIT; \
	else \
		$(CARGO) heather; \
	fi

license-fix: license-install
	$(CARGO) heather --fix

compile:
	$(CARGO) build

compile-test:
	$(CARGO) test --locked --no-run --all-targets

build-release:
	$(CARGO) build --release --locked

assert-rust-host:
	@set -euo pipefail; \
	actual_host="$$( $(RUSTC) -vV | awk '/^host:/ { print $$2 }' )"; \
	if [[ -z "$$actual_host" ]]; then \
		echo "Could not determine the Rust host triple" >&2; \
		exit 1; \
	fi; \
	if [[ -n "$(EXPECTED_RUST_HOST)" && "$$actual_host" != "$(EXPECTED_RUST_HOST)" ]]; then \
		echo "Expected Rust host $(EXPECTED_RUST_HOST), found $$actual_host" >&2; \
		exit 1; \
	fi; \
	echo "Rust host: $$actual_host"

test-unit:
	$(CARGO) test --lib

test-lib:
	@set -euo pipefail; \
	count="$$( $(CARGO) test --locked --lib -- --list | awk '/: test$$/ { count += 1 } END { print count + 0 }' )"; \
	if [[ "$$count" -ne "$(EXPECTED_LIB_TESTS)" ]]; then \
		echo "Expected $(EXPECTED_LIB_TESTS) library tests, found $$count"; \
		exit 1; \
	fi; \
	$(CARGO) test --locked --lib

test-doctests:
	@set -euo pipefail; \
	count="$$( $(CARGO) test --locked --doc -- --list | awk '/: test$$/ { count += 1 } END { print count + 0 }' )"; \
	if [[ "$$count" -ne "$(EXPECTED_DOCTESTS)" ]]; then \
		echo "Expected $(EXPECTED_DOCTESTS) doctests, found $$count"; \
		exit 1; \
	fi; \
	$(CARGO) test --locked --doc

test-hermetic:
	$(CARGO) test --locked --test non_success_connection_reuse_tests

test-dynamodb-compatibility:
	RUSTDOC_TOOLCHAIN="$(RUSTDOC_TOOLCHAIN)" $(CARGO) test --locked --test dynamodb_compatibility

test-native-certs:
	$(CARGO) test --locked --test native_ca_discovery

test-downstream:
	@set -euo pipefail; \
	consumer_root="$$(mktemp -d)"; \
	trap 'rm -rf "$$consumer_root"' EXIT; \
	$(CARGO_PLAIN) new --quiet --bin "$$consumer_root/consumer"; \
	$(CARGO_PLAIN) add --quiet --manifest-path "$$consumer_root/consumer/Cargo.toml" --path "$(CURDIR)"; \
	printf '%s\n' \
		'use alternator_driver::AlternatorConfig;' \
		'fn main() {' \
		'    let _config = AlternatorConfig::builder().seed_hosts(["127.0.0.1"]).build();' \
		'}' >"$$consumer_root/consumer/src/main.rs"; \
	$(CARGO_PLAIN) build --locked --manifest-path "$$consumer_root/consumer/Cargo.toml"

test-portable:
	$(MAKE) assert-rust-host
	$(MAKE) compile-test
	$(MAKE) test-lib
	$(MAKE) test-hermetic
	$(MAKE) test-doctests
	$(MAKE) test-dynamodb-compatibility
	$(MAKE) build-release
	$(MAKE) test-native-certs
	$(MAKE) test-downstream

test-integration: .prepare-ccm .prepare-environment-update-aio-max-nr
	@set -euo pipefail; \
	trap '$(CCM) remove "$(CCM_CLUSTER)"' EXIT; \
	$(MAKE) scylla-start; \
	$(MAKE) wait-for-alternator; \
	ALTERNATOR_TEST_ADDRESS="$(ALTERNATOR_TEST_ADDRESS)" $(CARGO) test --tests

test-server: .prepare-ccm .prepare-environment-update-aio-max-nr
	@set -euo pipefail; \
	trap '$(CCM) remove "$(CCM_CLUSTER)"' EXIT; \
	$(MAKE) scylla-start; \
	$(MAKE) wait-for-alternator; \
	server_test_args=(); \
	if [[ "$(SCYLLA_ALTERNATOR_HTTP_COMPRESSION)" != true ]]; then \
		server_test_args=( \
			--skip http_content::body_compression::test_request_compression_deflate \
			--skip http_content::body_compression::test_request_compression_gzip \
			--skip http_content::body_compression::test_enabled_by_per_request_customization \
			--skip http_content::body_compression::test_response_decompression_deflate \
			--skip http_content::body_compression::test_response_decompression_gzip \
		); \
	fi; \
	ALTERNATOR_TEST_ADDRESS="$(ALTERNATOR_TEST_ADDRESS)" $(CARGO) test --locked \
		--test http_content_tests --test https_test_tests -- "$${server_test_args[@]}"

test-ccm: .prepare-ccm .prepare-environment-update-aio-max-nr
	CCM_SCYLLA_VERSION="$(CCM_SCYLLA_VERSION)" RUSTFLAGS="$(RUSTFLAGS_CCM)" \
		$(CARGO) test --locked --test ccm_wrapper_tests -- --nocapture
	CCM_SCYLLA_VERSION="$(CCM_SCYLLA_VERSION)" RUSTFLAGS="$(RUSTFLAGS_CCM)" \
		$(CARGO) test --locked --test load_balancing_tests -- --nocapture

test-all:
	$(MAKE) test-portable
	$(MAKE) test-server
	$(MAKE) test-ccm

security-checks:
	$(CARGO) audit --deny warnings
	$(CARGO) deny check licenses sources

semver-check:
	@set -euo pipefail; \
	package_name="$(PACKAGE_NAME)"; \
	index_name="$$(printf '%s' "$$package_name" | tr '[:upper:]' '[:lower:]')"; \
	case "$${#index_name}" in \
		1) index_path="1/$$index_name" ;; \
		2) index_path="2/$$index_name" ;; \
		3) index_path="3/$${index_name:0:1}/$$index_name" ;; \
		*) index_path="$${index_name:0:2}/$${index_name:2:2}/$$index_name" ;; \
	esac; \
	index_file="$$(mktemp)"; \
	trap 'rm -f "$$index_file"' EXIT; \
	index_base="$(CRATES_IO_SPARSE_INDEX)"; \
	if ! http_status="$$( $(CURL) --silent --show-error --location \
		--user-agent "$(PACKAGE_NAME)-ci" \
		--output "$$index_file" --write-out '%{http_code}' \
		"$${index_base%/}/$$index_path" )"; then \
		echo "Failed to query the crates.io sparse index for $$package_name" >&2; \
		exit 1; \
	fi; \
	case "$$http_status" in \
		200) \
			if [[ ! -s "$$index_file" ]] || \
				! grep -Fq "\"name\":\"$$package_name\"" "$$index_file"; then \
				echo "Sparse index returned no valid records for $$package_name" >&2; \
				exit 1; \
			fi; \
			$(CARGO) semver-checks check-release \
			;; \
		404) \
			echo "Skipping semver check: $$package_name has no published crates.io records" \
			;; \
		*) \
			echo "Sparse index query for $$package_name returned HTTP $$http_status" >&2; \
			exit 1 \
			;; \
	esac

package-dry-run:
	$(CARGO) publish --dry-run --locked --target-dir "$$(mktemp -d)"

package-inspect:
	@set -euo pipefail; \
	candidate_crate="$(CANDIDATE_CRATE)"; \
	if [[ -z "$$candidate_crate" ]]; then \
		package_target="$$(mktemp -d)"; \
		$(CARGO) package --locked --no-verify --target-dir "$$package_target"; \
		candidate_crate="$$package_target/package/$(PACKAGE_NAME)-$(PACKAGE_VERSION).crate"; \
	fi; \
	test -f "$$candidate_crate"; \
	test -f "$(PACKAGE_INSPECT_SCRIPT)"; \
	bash "$(PACKAGE_INSPECT_SCRIPT)" "$(PACKAGE_VERSION)" "$$candidate_crate"

package-check:
	$(MAKE) package-dry-run
	$(MAKE) package-inspect
	$(MAKE) semver-check

wait-for-alternator:
	$(WAIT_FOR_ALTERNATOR_COMMANDS)

.prepare-environment-update-aio-max-nr:
	@if [[ -r /proc/sys/fs/aio-max-nr ]] && (( $$(< /proc/sys/fs/aio-max-nr) < 2097152 )); then
		echo 2097152 | sudo tee /proc/sys/fs/aio-max-nr >/dev/null
	fi

.prepare-ccm:
	@command -v "$(CCM)" >/dev/null || { echo "ccm is required; install scylla-ccm first"; exit 127; }

scylla-start: .prepare-ccm .prepare-environment-update-aio-max-nr
	$(SCYLLA_START_COMMANDS)

scylla-stop: .prepare-ccm
	$(CCM) switch "$(CCM_CLUSTER)"
	$(CCM) stop

scylla-kill: .prepare-ccm
	$(CCM) switch "$(CCM_CLUSTER)"
	$(CCM) stop --not-gently

scylla-rm: .prepare-ccm
	$(CCM) remove "$(CCM_CLUSTER)"

logs: .prepare-ccm
	$(CCM) switch "$(CCM_CLUSTER)"
	$(CCM) "$(CCM_NODE)" showlog

cqlsh: .prepare-ccm
	$(CCM) switch "$(CCM_CLUSTER)"
	$(CCM) "$(CCM_NODE)" cqlsh
