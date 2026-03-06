# ==========================================
# Skill Radar Makefile
# ==========================================
# Default: quiet, structured output.
# Verbose: make <target> VERBOSE=1
# ==========================================

.DEFAULT_GOAL := help
SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c

VERBOSE ?= 0
LOG_DIR := logs
SPARK_LOG := $(LOG_DIR)/spark_smoke.$(shell date +%Y%m%d_%H%M%S).log

# Silent prefix handled by Make (must be at the start of a recipe line)
ifeq ($(VERBOSE),1)
SILENT :=
else
SILENT := @
endif

# ---------- UI helpers (emit shell code; caller prefixes with $(SILENT)) ----------
C_RESET := \033[0m
C_BOLD  := \033[1m
C_BLUE  := \033[1;34m
C_GREEN := \033[1;32m
C_YELL  := \033[1;33m
C_RED   := \033[1;31m
C_DIM   := \033[2m

define UI_INFO
printf "$(C_BLUE)▶$(C_RESET) %s\n" "$(1)"
endef
define UI_OK
printf "$(C_GREEN)✓$(C_RESET) %s\n" "$(1)"
endef
define UI_WARN
printf "$(C_YELL)⚠$(C_RESET) %s\n" "$(1)"
endef
define UI_FAIL
printf "$(C_RED)✗$(C_RESET) %s\n" "$(1)"
endef

# Run a step with optional log capture path:
# $(call RUN_STEP,<label>,<logfile or empty>,<command>)
define RUN_STEP
$(SILENT)bash -lc '\
set -euo pipefail; \
STEP="$(1)"; LOGFILE="$(2)"; \
$(call UI_INFO,$(1)); \
if [[ -n "$$LOGFILE" ]]; then mkdir -p "$(LOG_DIR)"; fi; \
CMD="$$1"; \
if [[ "$(VERBOSE)" == "1" ]]; then \
  printf "$(C_DIM)• cmd: %s$(C_RESET)\n" "$$CMD"; \
fi; \
if bash -lc "$$CMD"; then \
  $(call UI_OK,$(1)); \
else \
  rc=$$?; \
  $(call UI_FAIL,$(1)); \
  echo ""; \
  printf "$(C_RED)Failure summary$(C_RESET)\n"; \
  printf "  • Step:  $(C_BOLD)%s$(C_RESET)\n" "$$STEP"; \
  if [[ -n "$$LOGFILE" ]]; then \
    printf "  • Logs:  $(C_BOLD)%s$(C_RESET)\n" "$$LOGFILE"; \
    printf "  • Tail:  (last 80 lines)\n"; \
    tail -n 80 "$$LOGFILE" || true; \
  else \
    printf "  • Logs:  (none captured)\n"; \
  fi; \
  echo ""; \
  printf "$(C_DIM)Tip: re-run with VERBOSE=1 for full command output.$(C_RESET)\n"; \
  exit $$rc; \
fi' -- '$(3)'
endef

# Run a host step with .env sourced (local-dev-only).
# Sources .env if present before running command.
# Required vars: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (for MinIO/S3 access)
# $(call RUN_STEP_HOST,<label>,<logfile or empty>,<command>)
define RUN_STEP_HOST
$(SILENT)bash -lc '\
set -euo pipefail; \
STEP="$(1)"; LOGFILE="$(2)"; \
$(call UI_INFO,$(1)); \
if [[ -n "$$LOGFILE" ]]; then mkdir -p "$(LOG_DIR)"; fi; \
CMD="$$1"; \
if [[ "$(VERBOSE)" == "1" ]]; then \
  printf "$(C_DIM)• cmd: %s$(C_RESET)\n" "$$CMD"; \
fi; \
ENV_CMD=""; \
if [[ -f .env ]]; then \
  ENV_CMD="set -a; source .env; set +a; "; \
  if [[ "$(VERBOSE)" == "1" ]]; then \
    printf "$(C_DIM)• sourced .env$(C_RESET)\n"; \
  fi; \
fi; \
if bash -lc "$${ENV_CMD}$$CMD"; then \
  $(call UI_OK,$(1)); \
else \
  rc=$$?; \
  $(call UI_FAIL,$(1)); \
  echo ""; \
  printf "$(C_RED)Failure summary$(C_RESET)\n"; \
  printf "  • Step:  $(C_BOLD)%s$(C_RESET)\n" "$$STEP"; \
  if [[ -n "$$LOGFILE" ]]; then \
    printf "  • Logs:  $(C_BOLD)%s$(C_RESET)\n" "$$LOGFILE"; \
    printf "  • Tail:  (last 80 lines)\n"; \
    tail -n 80 "$$LOGFILE" || true; \
  else \
    printf "  • Logs:  (none captured)\n"; \
  fi; \
  echo ""; \
  printf "$(C_DIM)Tip: re-run with VERBOSE=1 for full command output.$(C_RESET)\n"; \
  exit $$rc; \
fi' -- '$(3)'
endef

# Capture Git SHA for versioning (e.g. in Spark jobs, logs)
GIT_SHA ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "")
export GIT_SHA

.PHONY: help
help: ## Show available commands
	@echo ""
	@echo "Skill Radar — available commands:"
	@echo ""
	@echo "Usage:"
	@echo "  make <target> [VERBOSE=1]"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "  \033[1m%-18s\033[0m %s\n", "Target", "Description"} \
		/^[a-zA-Z0-9_.-]+:.*##/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""

# ==========================================
# Python Tooling
# ==========================================

.PHONY: bootstrap fix lint fmt type test check quality precommit

bootstrap: ## Install dev deps, install editable package, install pre-commit, run doctor
	$(call RUN_STEP,uv sync --dev,,uv sync --dev)
	$(call RUN_STEP,install editable package,,uv pip install -e .)
	$(call RUN_STEP,install pre-commit hook,,uv run pre-commit install)
	$(call RUN_STEP,doctor (full),,./scripts/doctor.sh)

fix: ## Auto-fix lint and format issues
	$(call RUN_STEP,ruff lint (fix),,uv run ruff check . --fix)
	$(call RUN_STEP,ruff format,,uv run ruff format .)

lint: ## Run ruff linter
	$(call RUN_STEP,ruff lint,,uv run ruff check .)

fmt: ## Check formatting
	$(call RUN_STEP,ruff format (check),,uv run ruff format --check .)

type: ## Run mypy type checking
	$(call RUN_STEP,mypy type-check,,uv run mypy src)

utest: ## Run unit tests
	$(call RUN_STEP,pytest (unit),,uv run pytest -q -m "not integration" --ignore=tests/integration)

test: utest ## Alias for utest

check: ## Run all local code checks
	$(SILENT)$(MAKE) lint VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) fmt VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) type VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) utest VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,All local checks passed.)'

quality: ## Run formatting fixes + core checks
	$(SILENT)$(MAKE) fix VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) lint VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) type VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) utest VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,Quality pipeline passed.)'

precommit: ## Run all pre-commit hooks
	$(call RUN_STEP,pre-commit (all hooks),,uv run pre-commit run --all-files)

# ==========================================
# Doctor
# ==========================================

.PHONY: doctor doctor-fix doctor-live doctor-infra-only
doctor: ## Run full doctor (local + docker infra + smoke)
	$(call RUN_STEP,doctor (full),,./scripts/doctor.sh)

doctor-fix: ## Doctor with auto-fix
	$(call RUN_STEP,doctor (--fix),,./scripts/doctor.sh --fix)

doctor-live: ## Doctor + live Adzuna API check
	$(call RUN_STEP,doctor (--live),,./scripts/doctor.sh --live)

doctor-infra-only: ## Only docker infra + smoke
	$(call RUN_STEP,doctor (--infra-only),,./scripts/doctor.sh --infra-only)

# ==========================================
# Docker Compose (Infrastructure)
# ==========================================

.PHONY: up down reset ps logs smoke itest test-all

up: ## Start local infrastructure
	$(call RUN_STEP,docker compose build spark,,docker compose build spark)
	$(call RUN_STEP,docker compose up -d,,docker compose up -d)

down: ## Stop infrastructure
	$(call RUN_STEP,docker compose down,,docker compose down)

reset: ## Stop infra and remove volumes
	$(call RUN_STEP,docker compose down -v,,docker compose down -v)

ps: ## Show running services
	$(call RUN_STEP,docker compose ps,,docker compose ps)

logs: ## Tail service logs (Ctrl+C to stop)
	@echo "Tailing docker logs (Ctrl+C to stop)..."
	@docker compose logs -f --tail=200

smoke: ## Run Spark + Iceberg smoke test (logs captured under logs/)
	@mkdir -p "$(LOG_DIR)"
	$(call RUN_STEP,Spark + Iceberg smoke test,$(SPARK_LOG),\
	docker compose exec -T spark bash -lc "spark-submit /opt/skillradar/jobs/examples/iceberg_smoke_test.py" \
	2>&1 | tee "$(SPARK_LOG)" >/dev/null)
	$(SILENT)bash -lc '$(call UI_OK,Smoke log saved to $(SPARK_LOG))'

itest: ## Run integration tests (requires docker infra up)
	$(call RUN_STEP,pytest (integration),,uv run pytest -m integration -q)

test-all: ## Run all tests (unit + integration)
	$(SILENT)$(MAKE) utest VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) itest VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,All tests passed.)'

# ==========================================
# Grouped Commands
# ==========================================

.PHONY: infra infra-reset ci dev

infra: ## Start infra and validate with smoke test
	$(SILENT)$(MAKE) up VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) smoke VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,Infrastructure is healthy.)'

infra-reset: ## Full reset + restart infra + smoke test
	$(SILENT)$(MAKE) reset VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) infra VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,Infrastructure reset complete.)'

ci: ## Run full CI pipeline (check + integration tests)
	$(SILENT)$(MAKE) check VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) itest VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,CI pipeline passed.)'

dev: ## Run quality checks and ensure infra is healthy
	$(SILENT)$(MAKE) quality VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) infra VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,Dev workflow passed.)'

# ==========================================
# Validation Commands
# ==========================================

# Dynamic overrides for ESCO pipeline
VERSION ?= v1.2.1
ESCO_LANG ?= fr
FILE ?= data/esco.zip
ENTITIES ?=
EXTRA ?=

# Helper: build optional --entities flag
_ENTITIES_FLAG = $(if $(ENTITIES),--entities $(ENTITIES),)

DOCKER_EXEC = docker compose exec -T
SPARK_EXEC  = $(DOCKER_EXEC) spark bash -lc

.PHONY: validate-infra validate-infra-runtime validate-infra-all validate-esco-landing validate-esco-bronze validate-esco-bronze-e2e validate-bronze validate-adzuna-bronze validate-adzuna-silver

validate-infra: ## Validate infrastructure (host scope: boto3 checks only) [sources .env]
	$(call RUN_STEP_HOST,Validate infrastructure (host),,\
	SKILLRADAR_RUNTIME_CONTEXT=host uv run skill-radar validate infra --scope host)

validate-infra-runtime: ## Validate infrastructure runtime (Spark/Iceberg checks) [Spark]
	$(call RUN_STEP,Validate infrastructure (runtime),,\
	$(SPARK_EXEC) "SKILLRADAR_RUNTIME_CONTEXT=docker uv run skill-radar validate infra --scope runtime")

validate-infra-all: ## Validate all infrastructure (host + runtime checks) [Spark]
	$(call RUN_STEP,Validate infrastructure (all),,\
	$(SPARK_EXEC) "SKILLRADAR_RUNTIME_CONTEXT=docker uv run skill-radar validate infra --scope all")

validate-esco-landing: ## Validate ESCO landing zone (VERSION=... ESCO_LANG=...)
	$(call RUN_STEP_HOST,Validate ESCO landing,,uv run skill-radar validate esco-landing --version $(VERSION) --lang $(ESCO_LANG))

validate-esco-bronze: ## Validate ESCO bronze tables (VERSION=... ESCO_LANG=... ENTITIES=...) [Spark]
	$(call RUN_STEP,Validate ESCO bronze (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar validate esco-bronze --version $(VERSION) --lang $(ESCO_LANG) $(_ENTITIES_FLAG) $(EXTRA)")

validate-esco-bronze-e2e: ## E2E bronze validation: extract + validate (VERSION=... ESCO_LANG=... ENTITIES=...) [Spark]
	$(call RUN_STEP,Validate ESCO bronze E2E (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar validate esco-bronze-e2e --version $(VERSION) --lang $(ESCO_LANG) $(_ENTITIES_FLAG) $(EXTRA)")

validate-bronze: ## Validate bronze tables (dataset=esco, VERSION=... ESCO_LANG=...)
	$(SILENT)$(MAKE) validate-esco-bronze VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) ENTITIES=$(ENTITIES) EXTRA=$(EXTRA) VERBOSE=$(VERBOSE)

# ─────────────────────────────────────────────────────────────────────────────
# Infrastructure Provisioning
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: apply-infra infra-status run-infra

apply-infra: ## Provision infrastructure (buckets + namespaces)
	$(call RUN_STEP,Apply infrastructure,, \
	$(SPARK_EXEC) "uv run skill-radar infra apply")

apply-infra-host: ## Provision infrastructure from host (buckets only, skips namespaces) [sources .env]
	$(call RUN_STEP_HOST,Apply infrastructure (host),,uv run skill-radar infra apply)

infra-status: ## Check infrastructure status (alias for validate-infra, host scope)
	$(SILENT)$(MAKE) validate-infra VERBOSE=$(VERBOSE)

run-infra: ## Apply + validate infrastructure
	$(call RUN_STEP,Run infrastructure (apply + validate),, \
	$(SPARK_EXEC) "uv run skill-radar run infra")

run-infra-host: ## Apply + validate infrastructure from host (buckets only) [sources .env]
	$(call RUN_STEP_HOST,Run infrastructure (host),,uv run skill-radar run infra)

# ─────────────────────────────────────────────────────────────────────────────
# ESCO Bronze Pipeline (3-step)
# ─────────────────────────────────────────────────────────────────────────────
# Usage (dropzone workflow - recommended):
#   1. Download ESCO ZIP manually to ./data/incoming/esco/esco.zip
#   2. make upload-esco VERSION=v1.2.1 ESCO_LANG=fr
#   3. make bronze-esco VERSION=v1.2.1 ESCO_LANG=fr
#   4. make validate-esco-bronze VERSION=v1.2.1 ESCO_LANG=fr
#
# Usage (direct file - host only):
#   make upload-esco-local FILE=data/esco.zip VERSION=v1.2.1 ESCO_LANG=fr
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: upload-esco upload-esco-local bronze-esco run-esco-bronze

upload-esco: ## Upload ESCO artifact from dropzone (docker): VERSION=... ESCO_LANG=...
	$(call RUN_STEP,Upload ESCO artifact (via dropzone in container),,\
	docker compose exec -T spark bash -lc "uv run skill-radar esco upload --version $(VERSION) --lang $(ESCO_LANG) $(EXTRA)")

upload-esco-local: ## Upload ESCO artifact (host-side, direct file): VERSION=... ESCO_LANG=... FILE=...
	$(call RUN_STEP,Upload ESCO artifact (host),,\
	uv run skill-radar esco upload --version $(VERSION) --lang $(ESCO_LANG) --file $(FILE) $(EXTRA))

bronze-esco: ## Run ESCO bronze extraction (Spark): VERSION=... ESCO_LANG=... ENTITIES=...
	$(call RUN_STEP,Run ESCO bronze extraction (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar esco bronze --version $(VERSION) --lang $(ESCO_LANG) $(_ENTITIES_FLAG) $(EXTRA)")

run-esco-bronze: ## Full ESCO bronze pipeline: upload → extract → validate (VERSION=... ESCO_LANG=...)
	$(SILENT)$(MAKE) upload-esco VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) bronze-esco VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) ENTITIES=$(ENTITIES) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) validate-esco-bronze VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) ENTITIES=$(ENTITIES) EXTRA=$(EXTRA) VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,ESCO bronze pipeline complete.)'

# ─────────────────────────────────────────────────────────────────────────────
# ESCO Silver Pipeline
# ─────────────────────────────────────────────────────────────────────────────
# Usage:
#   1. Ensure bronze tables are populated (make run-esco-bronze VERSION=v1.2.1 ESCO_LANG=fr)
#   2. make silver-esco VERSION=v1.2.1 ESCO_LANG=fr
#   3. make validate-esco-silver VERSION=v1.2.1 ESCO_LANG=fr
# ─────────────────────────────────────────────────────────────────────────────

.PHONY: silver-esco validate-esco-silver run-esco-silver

silver-esco: ## Run ESCO silver formatting (Spark): VERSION=... ESCO_LANG=... ENTITIES=...
	$(call RUN_STEP,Run ESCO silver formatting (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar esco silver --version $(VERSION) --lang $(ESCO_LANG) $(_ENTITIES_FLAG) $(EXTRA)")

validate-esco-silver: ## Validate ESCO silver tables (VERSION=... ESCO_LANG=... ENTITIES=...) [Spark]
	$(call RUN_STEP,Validate ESCO silver (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar validate esco-silver --version $(VERSION) --lang $(ESCO_LANG) $(_ENTITIES_FLAG) $(EXTRA)")

run-esco-silver: ## Full ESCO silver pipeline: format → validate (VERSION=... ESCO_LANG=...)
	$(SILENT)$(MAKE) silver-esco VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) ENTITIES=$(ENTITIES) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) validate-esco-silver VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) ENTITIES=$(ENTITIES) EXTRA=$(EXTRA) VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,ESCO silver pipeline complete.)'

# ─────────────────────────────────────────────────────────────────────────────
# Adzuna Pipeline
# ─────────────────────────────────────────────────────────────────────────────
# Environment:
#   ADZUNA_APP_ID  — Adzuna API application ID  (required; set in .env or export)
#   ADZUNA_APP_KEY — Adzuna API application key  (required; set in .env or export)
#
# Usage (bronze — API extraction):
#   make adzuna-bronze                           # defaults: country=fr, preset=default_fr
#   make adzuna-bronze ADZUNA_COUNTRY=gb ADZUNA_MAX_PAGES=5
#
# Usage (silver — format + deduplicate):
#   make adzuna-silver                           # defaults: country=fr
#   make adzuna-silver ADZUNA_COUNTRY=fr ADZUNA_INGESTION_DATE=2025-01-15
#
# Usage (full pipeline):
#   make run-adzuna                              # bronze → validate → silver → validate
# ─────────────────────────────────────────────────────────────────────────────

# Dynamic overrides for Adzuna pipeline
ADZUNA_COUNTRY          ?= fr
ADZUNA_PRESET           ?= default_fr
ADZUNA_MAX_PAGES        ?=
ADZUNA_RESULTS_PER_PAGE ?=
ADZUNA_INGESTION_DATE   ?=

# Helper: build optional Adzuna CLI flags
_ADZUNA_MAX_PAGES_FLAG = $(if $(ADZUNA_MAX_PAGES),--max-pages $(ADZUNA_MAX_PAGES),)
_ADZUNA_RPP_FLAG       = $(if $(ADZUNA_RESULTS_PER_PAGE),--results-per-page $(ADZUNA_RESULTS_PER_PAGE),)
_ADZUNA_DATE_FLAG      = $(if $(ADZUNA_INGESTION_DATE),--ingestion-date $(ADZUNA_INGESTION_DATE),)

.PHONY: adzuna-bronze adzuna-silver validate-adzuna-bronze validate-adzuna-silver run-adzuna

adzuna-bronze: ## Run Adzuna bronze extraction (Spark): ADZUNA_COUNTRY=... ADZUNA_PRESET=...
	$(call RUN_STEP,Run Adzuna bronze extraction (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar adzuna bronze --preset $(ADZUNA_PRESET) --country $(ADZUNA_COUNTRY) $(_ADZUNA_MAX_PAGES_FLAG) $(_ADZUNA_RPP_FLAG) $(EXTRA)")

adzuna-silver: ## Run Adzuna silver formatting (Spark): ADZUNA_COUNTRY=... ADZUNA_INGESTION_DATE=...
	$(call RUN_STEP,Run Adzuna silver formatting (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar adzuna silver --country $(ADZUNA_COUNTRY) $(_ADZUNA_DATE_FLAG) $(EXTRA)")

validate-adzuna-bronze: ## Validate Adzuna bronze tables [Spark]
	$(call RUN_STEP,Validate Adzuna bronze (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar validate adzuna-bronze $(EXTRA)")

validate-adzuna-silver: ## Validate Adzuna silver tables [Spark]
	$(call RUN_STEP,Validate Adzuna silver (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar validate adzuna-silver $(EXTRA)")

run-adzuna: ## Full Adzuna pipeline: bronze → validate → silver → validate
	$(SILENT)$(MAKE) adzuna-bronze ADZUNA_COUNTRY=$(ADZUNA_COUNTRY) ADZUNA_PRESET=$(ADZUNA_PRESET) ADZUNA_MAX_PAGES=$(ADZUNA_MAX_PAGES) ADZUNA_RESULTS_PER_PAGE=$(ADZUNA_RESULTS_PER_PAGE) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) validate-adzuna-bronze EXTRA=$(EXTRA) VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) adzuna-silver ADZUNA_COUNTRY=$(ADZUNA_COUNTRY) ADZUNA_INGESTION_DATE=$(ADZUNA_INGESTION_DATE) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) validate-adzuna-silver EXTRA=$(EXTRA) VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,Adzuna pipeline complete.)'

# ─────────────────────────────────────────────────────────────────────────────
# Gold Pipeline
# ─────────────────────────────────────────────────────────────────────────────
# Requires:
#   - ESCO Silver tables populated (make run-esco-silver VERSION=... ESCO_LANG=...)
#   - Adzuna Silver table populated (make run-adzuna ADZUNA_COUNTRY=...)
#
# Usage:
#   make gold-matching  GOLD_COUNTRY=fr GOLD_INGESTION_DATE=2025-01-15 GOLD_ESCO_VERSION=v1.2.1 GOLD_ESCO_LANG=fr
#   make gold-analytics GOLD_COUNTRY=fr GOLD_INGESTION_DATE=2025-01-15 GOLD_ESCO_VERSION=v1.2.1 GOLD_ESCO_LANG=fr
#   make validate-gold  GOLD_COUNTRY=fr GOLD_INGESTION_DATE=2025-01-15 GOLD_ESCO_VERSION=v1.2.1 GOLD_ESCO_LANG=fr
#   make run-gold        # matching → analytics → validate
# ─────────────────────────────────────────────────────────────────────────────

# Dynamic overrides for Gold pipeline
GOLD_COUNTRY          ?= fr
GOLD_INGESTION_DATE   ?=
GOLD_ESCO_VERSION     ?= v1.2.1
GOLD_ESCO_LANG        ?= fr
GOLD_JOB_LIMIT        ?=

# Helper: build optional Gold CLI flags
_GOLD_DATE_FLAG      = $(if $(GOLD_INGESTION_DATE),--ingestion-date $(GOLD_INGESTION_DATE),)
_GOLD_LIMIT_FLAG     = $(if $(GOLD_JOB_LIMIT),--job-limit $(GOLD_JOB_LIMIT),)

.PHONY: gold-matching gold-analytics validate-gold run-gold

gold-matching: ## Run Gold matching (Spark): GOLD_COUNTRY=... GOLD_INGESTION_DATE=... GOLD_ESCO_VERSION=... GOLD_ESCO_LANG=...
	$(call RUN_STEP,Run Gold matching (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar gold matching $(_GOLD_DATE_FLAG) --country $(GOLD_COUNTRY) --esco-version $(GOLD_ESCO_VERSION) --esco-lang $(GOLD_ESCO_LANG) $(_GOLD_LIMIT_FLAG) $(EXTRA)")

gold-analytics: ## Run Gold analytics (Spark): GOLD_COUNTRY=... GOLD_INGESTION_DATE=... GOLD_ESCO_VERSION=... GOLD_ESCO_LANG=...
	$(call RUN_STEP,Run Gold analytics (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar gold analytics $(_GOLD_DATE_FLAG) --country $(GOLD_COUNTRY) --esco-version $(GOLD_ESCO_VERSION) --esco-lang $(GOLD_ESCO_LANG) $(EXTRA)")

validate-gold: ## Validate Gold tables (Spark): GOLD_COUNTRY=... GOLD_INGESTION_DATE=... GOLD_ESCO_VERSION=... GOLD_ESCO_LANG=...
	$(call RUN_STEP,Validate Gold tables (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar validate gold $(_GOLD_DATE_FLAG) --country $(GOLD_COUNTRY) --esco-version $(GOLD_ESCO_VERSION) --esco-lang $(GOLD_ESCO_LANG) $(EXTRA)")

run-gold: ## Full Gold pipeline: matching → analytics → validate
	$(SILENT)$(MAKE) gold-matching GOLD_COUNTRY=$(GOLD_COUNTRY) GOLD_INGESTION_DATE=$(GOLD_INGESTION_DATE) GOLD_ESCO_VERSION=$(GOLD_ESCO_VERSION) GOLD_ESCO_LANG=$(GOLD_ESCO_LANG) GOLD_JOB_LIMIT=$(GOLD_JOB_LIMIT) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) gold-analytics GOLD_COUNTRY=$(GOLD_COUNTRY) GOLD_INGESTION_DATE=$(GOLD_INGESTION_DATE) GOLD_ESCO_VERSION=$(GOLD_ESCO_VERSION) GOLD_ESCO_LANG=$(GOLD_ESCO_LANG) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) validate-gold GOLD_COUNTRY=$(GOLD_COUNTRY) GOLD_INGESTION_DATE=$(GOLD_INGESTION_DATE) GOLD_ESCO_VERSION=$(GOLD_ESCO_VERSION) GOLD_ESCO_LANG=$(GOLD_ESCO_LANG) EXTRA=$(EXTRA) VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,Gold pipeline complete.)'
