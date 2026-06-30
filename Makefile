SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eo pipefail -c

CARGO ?= cargo
RUSTFLAGS_CCM ?= --cfg ccm_tests

MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
BIN := $(MAKEFILE_PATH)/bin
OS := $(shell uname | tr '[:upper:]' '[:lower:]')
ARCH := $(shell uname -m)
DOCKER_COMPOSE_ARCH := $(ARCH)
DOCKER_COMPOSE_VERSION := 2.34.0
REPO_DOCKER_COMPOSE := $(BIN)/docker-compose
SYSTEM_DOCKER_COMPOSE := $(shell \
	if docker compose version >/dev/null 2>&1; then \
		printf 'docker compose'; \
	elif command -v docker-compose >/dev/null 2>&1; then \
		printf 'docker-compose'; \
	fi)

ifeq ($(ARCH),arm64)
	DOCKER_COMPOSE_ARCH := aarch64
else ifeq ($(ARCH),amd64)
	DOCKER_COMPOSE_ARCH := x86_64
endif

ifeq ($(DOCKER_COMPOSE_ARCH),aarch64)
	DOCKER_COMPOSE_DOWNLOAD_URL := https://github.com/docker/compose/releases/download/v$(DOCKER_COMPOSE_VERSION)/docker-compose-$(OS)-aarch64
else ifeq ($(DOCKER_COMPOSE_ARCH),x86_64)
	DOCKER_COMPOSE_DOWNLOAD_URL := https://github.com/docker/compose/releases/download/v$(DOCKER_COMPOSE_VERSION)/docker-compose-$(OS)-x86_64
endif

DOCKER_COMPOSE ?= $(if $(SYSTEM_DOCKER_COMPOSE),$(SYSTEM_DOCKER_COMPOSE),$(REPO_DOCKER_COMPOSE))
COMPOSE = $(DOCKER_COMPOSE) -f $(MAKEFILE_PATH)/docker-compose.yml
SCYLLA_IMAGE := scylladb/scylla
DOCKER_CACHE_DIR := $(MAKEFILE_PATH)/.docker-cache
DOCKER_CACHE_FILE := $(DOCKER_CACHE_DIR)/scylla-image.tar

.PHONY: clean verify lint lint-docs lint-fix compile compile-test
.PHONY: test-unit test-integration test-all
.PHONY: wait-for-alternator scylla-start scylla-stop scylla-kill scylla-rm
.PHONY: docker-pull docker-cache-save docker-cache-load
.PHONY: logs cqlsh shell volumes prune

lint:
	$(CARGO) fmt --all -- --check
	$(CARGO) check --all-targets
	$(CARGO) clippy --all-targets -- -D warnings
	$(CARGO) doc --no-deps

clean:
	$(CARGO) clean

verify: lint test-all

lint-docs:
	$(CARGO) doc --no-deps

lint-fix:
	$(CARGO) fmt --all

compile:
	$(CARGO) build

compile-test:
	$(CARGO) test --no-run --all-targets

test-unit:
	$(CARGO) test --lib

test-integration: scylla-start wait-for-alternator
	trap '$(COMPOSE) down --remove-orphans -v' EXIT
	$(CARGO) test --tests

test-all: scylla-start wait-for-alternator
	trap '$(COMPOSE) down --remove-orphans -v' EXIT
	$(CARGO) test
	$(COMPOSE) down --remove-orphans -v
	trap - EXIT
	RUSTFLAGS="$(RUSTFLAGS_CCM)" $(CARGO) test --test ccm_wrapper_tests -- --nocapture
	RUSTFLAGS="$(RUSTFLAGS_CCM)" $(CARGO) test --test load_balancing_tests -- --nocapture

wait-for-alternator:
	echo "Waiting for Alternator to be ready..."
	for i in $$(seq 1 60); do
		if curl -sf http://localhost:8000/localnodes >/dev/null 2>&1; then
			echo "Alternator is ready (waited $${i}s)"
			exit 0
		fi
		sleep 1
	done
	echo "Timed out waiting for Alternator"
	exit 1

.prepare-environment-update-aio-max-nr:
	@if [[ -r /proc/sys/fs/aio-max-nr ]] && (( $$(< /proc/sys/fs/aio-max-nr) < 2097152 )); then
		echo 2097152 | sudo tee /proc/sys/fs/aio-max-nr >/dev/null
	fi

.prepare-docker-compose:
	@if [[ "$(DOCKER_COMPOSE)" != "$(REPO_DOCKER_COMPOSE)" ]]; then
		echo "Using docker compose: $(DOCKER_COMPOSE)"
		exit 0
	fi
	if [[ -z "$(DOCKER_COMPOSE_DOWNLOAD_URL)" ]]; then
		echo "Unable to download docker-compose for unsupported architecture \"$(ARCH)\""
		exit 69
	fi
	[ -d "$(BIN)" ] || mkdir "$(BIN)"
	if [[ -f "$(REPO_DOCKER_COMPOSE)" ]] && "$(REPO_DOCKER_COMPOSE)" version 2>/dev/null | grep "$(DOCKER_COMPOSE_VERSION)" >/dev/null; then
		echo "docker-compose $(DOCKER_COMPOSE_VERSION) is already installed"
	else
		echo "Downloading $(REPO_DOCKER_COMPOSE)"
		curl --fail --show-error --progress-bar -L "$(DOCKER_COMPOSE_DOWNLOAD_URL)" --output "$(REPO_DOCKER_COMPOSE)"
		chmod +x "$(REPO_DOCKER_COMPOSE)"
	fi

.prepare-bin:
	@[ -d "$(BIN)" ] || mkdir "$(BIN)"

scylla-start: .prepare-docker-compose .prepare-environment-update-aio-max-nr docker-cache-load
	$(COMPOSE) up -d --wait

scylla-stop: .prepare-docker-compose
	$(COMPOSE) down --remove-orphans -v

scylla-kill: .prepare-docker-compose
	$(COMPOSE) kill

scylla-rm: .prepare-docker-compose
	$(COMPOSE) rm -f

docker-pull:
	docker pull $(SCYLLA_IMAGE)

docker-cache-save: docker-pull
	@mkdir -p $(DOCKER_CACHE_DIR)
	docker save $(SCYLLA_IMAGE) -o $(DOCKER_CACHE_FILE)

docker-cache-load:
	@if [ -f "$(DOCKER_CACHE_FILE)" ]; then
		echo "Loading Docker image from cache..."
		docker load -i $(DOCKER_CACHE_FILE)
	else
		echo "Cache file not found, pulling image..."
		docker pull $(SCYLLA_IMAGE)
	fi

logs: .prepare-docker-compose
	$(COMPOSE) logs -f

cqlsh: .prepare-docker-compose
	$(COMPOSE) exec scylla_node cqlsh -u cassandra -p cassandra

shell: .prepare-docker-compose
	$(COMPOSE) exec scylla_node bash

volumes:
	docker volume ls

prune:
	docker system prune -a --volumes
