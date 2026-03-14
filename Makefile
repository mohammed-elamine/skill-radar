# Skill Radar Makefile — single developer interface.
# Usage: make <target> [VERBOSE=1]

.DEFAULT_GOAL := help
SHELL := /usr/bin/env bash
.SHELLFLAGS := -euo pipefail -c

VERBOSE ?= 0
LOG_DIR := logs
SPARK_LOG := $(LOG_DIR)/spark_smoke.$(shell date +%Y%m%d_%H%M%S).log

ifeq ($(VERBOSE),1)
SILENT :=
else
SILENT := @
endif

# ── UI helpers ────────────────────────────────────────────────────────────
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

# $(call RUN_STEP,<label>,<logfile|empty>,<command>)
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

# Like RUN_STEP but sources .env before running (for host-side S3 access).
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

GIT_SHA ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "")
export GIT_SHA

# ── Shared variables ──────────────────────────────────────────────────────

DOCKER_EXEC = docker compose exec -T
SPARK_EXEC  = $(DOCKER_EXEC) spark bash -lc
ALL_PROFILES = docker compose --profile airflow --profile search

# ESCO overrides
VERSION   ?= v1.2.1
ESCO_LANG ?= fr
FILE      ?= data/esco.zip
ENTITIES  ?=
EXTRA     ?=
_ENTITIES_FLAG = $(if $(ENTITIES),--entities $(ENTITIES),)

# Adzuna overrides
ADZUNA_COUNTRY          ?= fr
ADZUNA_PRESET           ?= default_fr
ADZUNA_MAX_PAGES        ?=
ADZUNA_RESULTS_PER_PAGE ?=
ADZUNA_INGESTION_DATE   ?=
_ADZUNA_MAX_PAGES_FLAG = $(if $(ADZUNA_MAX_PAGES),--max-pages $(ADZUNA_MAX_PAGES),)
_ADZUNA_RPP_FLAG       = $(if $(ADZUNA_RESULTS_PER_PAGE),--results-per-page $(ADZUNA_RESULTS_PER_PAGE),)
_ADZUNA_DATE_FLAG      = $(if $(ADZUNA_INGESTION_DATE),--ingestion-date $(ADZUNA_INGESTION_DATE),)

# Gold overrides
GOLD_COUNTRY        ?= fr
GOLD_INGESTION_DATE ?=
GOLD_ESCO_VERSION   ?= v1.2.1
GOLD_ESCO_LANG      ?= fr
GOLD_JOB_LIMIT      ?=
_GOLD_DATE_FLAG  = $(if $(GOLD_INGESTION_DATE),--ingestion-date $(GOLD_INGESTION_DATE),)
_GOLD_LIMIT_FLAG = $(if $(GOLD_JOB_LIMIT),--job-limit $(GOLD_JOB_LIMIT),)

# Search overrides
SEARCH_COUNTRY        ?= fr
SEARCH_INGESTION_DATE ?=
SEARCH_DATASET        ?=
SEARCH_ES_URL         ?=
SEARCH_KIBANA_URL     ?=
SEARCH_ES_URL_DOCKER     ?= http://elasticsearch:9200
SEARCH_ES_URL_HOST       ?= http://localhost:9200
SEARCH_KIBANA_URL_DOCKER ?= http://kibana:5601
SEARCH_KIBANA_URL_HOST   ?= http://localhost:5601
SEARCH_COMPOSE = docker compose --profile search
_SEARCH_DATE_FLAG    = $(if $(SEARCH_INGESTION_DATE),--ingestion-date $(SEARCH_INGESTION_DATE),)
_SEARCH_DATASET_FLAG = $(if $(SEARCH_DATASET),--dataset $(SEARCH_DATASET),)
_SEARCH_ES_URL_FLAG_HOST     = --es-url $(if $(SEARCH_ES_URL),$(SEARCH_ES_URL),$(SEARCH_ES_URL_HOST))
_SEARCH_KIBANA_URL_FLAG_HOST = --kibana-url $(if $(SEARCH_KIBANA_URL),$(SEARCH_KIBANA_URL),$(SEARCH_KIBANA_URL_HOST))
_SEARCH_ES_URL_FLAG_DOCKER     = --es-url $(if $(SEARCH_ES_URL),$(SEARCH_ES_URL),$(SEARCH_ES_URL_DOCKER))
_SEARCH_KIBANA_URL_FLAG_DOCKER = --kibana-url $(if $(SEARCH_KIBANA_URL),$(SEARCH_KIBANA_URL),$(SEARCH_KIBANA_URL_DOCKER))

# Airflow overrides
AIRFLOW_ADZUNA_DATE  ?= $(shell date +%Y-%m-%d)
AIRFLOW_ESCO_VERSION ?= v1.2.1
AIRFLOW_ESCO_LANG    ?= fr
AIRFLOW_ESCO_RUN_GOLD ?= false
AIRFLOW_COMPOSE = docker compose --profile airflow
AIRFLOW_EXEC    = $(AIRFLOW_COMPOSE) exec -T airflow-scheduler

# Full pipeline overrides
PIPELINE_DATE    ?= $(shell date +%Y-%m-%d)
PIPELINE_COUNTRY ?= fr

# ── Help ──────────────────────────────────────────────────────────────────

.PHONY: help
help: ## Show available commands
	@echo ""
	@echo "Skill Radar — available commands:"
	@echo ""
	@echo "Usage:  make <target> [VERBOSE=1]"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"; printf "  \033[1m%-24s\033[0m %s\n", "Target", "Description"} \
		/^[a-zA-Z0-9_.-]+:.*##/ {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""

# ══════════════════════════════════════════════════════════════════════════
# Development & Quality
# ══════════════════════════════════════════════════════════════════════════

.PHONY: bootstrap fix lint fmt type utest test check quality precommit

bootstrap: ## Install deps, editable package, pre-commit hooks, run doctor
	$(call RUN_STEP,uv sync --dev,,uv sync --dev)
	$(call RUN_STEP,install editable package,,uv pip install -e .)
	$(call RUN_STEP,install pre-commit hook,,uv run pre-commit install)
	$(call RUN_STEP,doctor (full),,./scripts/doctor.sh)

fix: ## Auto-fix lint and format issues
	$(call RUN_STEP,ruff lint (fix),,uv run ruff check . --fix)
	$(call RUN_STEP,ruff format,,uv run ruff format .)

lint: ## Run ruff linter
	$(call RUN_STEP,ruff lint,,uv run ruff check .)

fmt: ## Check formatting (no changes)
	$(call RUN_STEP,ruff format (check),,uv run ruff format --check .)

type: ## Run mypy type checking
	$(call RUN_STEP,mypy type-check,,uv run mypy src)

utest: ## Run unit tests
	$(call RUN_STEP,pytest (unit),,uv run pytest -q -m "not integration" --ignore=tests/integration)

test: utest ## Alias for utest

check: ## Run all local code checks (lint + format + types + unit tests)
	$(SILENT)$(MAKE) lint VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) fmt VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) type VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) utest VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,All local checks passed.)'

quality: ## Auto-fix then run checks
	$(SILENT)$(MAKE) fix VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) lint VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) type VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) utest VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,Quality pipeline passed.)'

precommit: ## Run all pre-commit hooks
	$(call RUN_STEP,pre-commit (all hooks),,uv run pre-commit run --all-files)

# ══════════════════════════════════════════════════════════════════════════
# Doctor
# ══════════════════════════════════════════════════════════════════════════

.PHONY: doctor doctor-fix doctor-live doctor-infra-only

doctor: ## Run full environment doctor (local + docker + smoke)
	$(call RUN_STEP,doctor (full),,./scripts/doctor.sh)

doctor-fix: ## Doctor with auto-fix
	$(call RUN_STEP,doctor (--fix),,./scripts/doctor.sh --fix)

doctor-live: ## Doctor + live Adzuna API check
	$(call RUN_STEP,doctor (--live),,./scripts/doctor.sh --live)

doctor-infra-only: ## Docker infra + smoke only
	$(call RUN_STEP,doctor (--infra-only),,./scripts/doctor.sh --infra-only)

# ══════════════════════════════════════════════════════════════════════════
# Infrastructure
# ══════════════════════════════════════════════════════════════════════════

.PHONY: up down reset ps logs smoke itest test-all infra infra-reset

up: ## Start core infrastructure (MinIO + Spark)
	$(call RUN_STEP,docker compose build spark,,docker compose build spark)
	$(call RUN_STEP,docker compose up -d,,docker compose up -d)

down: ## Stop core infrastructure
	$(call RUN_STEP,docker compose down,,docker compose down)

reset: ## Stop infra and remove volumes
	$(call RUN_STEP,docker compose down -v,,docker compose down -v)

ps: ## Show running services
	$(call RUN_STEP,docker compose ps,,docker compose ps)

logs: ## Tail service logs
	@docker compose logs -f --tail=200

smoke: ## Run Spark + Iceberg smoke test
	@mkdir -p "$(LOG_DIR)"
	$(call RUN_STEP,Spark + Iceberg smoke test,$(SPARK_LOG),\
	docker compose exec -T spark bash -lc "spark-submit /opt/skillradar/jobs/examples/iceberg_smoke_test.py" \
	2>&1 | tee "$(SPARK_LOG)" >/dev/null)
	$(SILENT)bash -lc '$(call UI_OK,Smoke log saved to $(SPARK_LOG))'

itest: ## Run integration tests (requires infra up)
	$(call RUN_STEP,pytest (integration),,uv run pytest -m integration -q)

test-all: ## Run all tests (unit + integration)
	$(SILENT)$(MAKE) utest VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) itest VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,All tests passed.)'

infra: ## Start infra + validate with smoke test
	$(SILENT)$(MAKE) up VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) smoke VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,Infrastructure is healthy.)'

infra-reset: ## Full reset + restart + smoke
	$(SILENT)$(MAKE) reset VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) infra VERBOSE=$(VERBOSE)

# ── Infrastructure provisioning ───────────────────────────────────────────

.PHONY: apply-infra apply-infra-host infra-status run-infra run-infra-host

apply-infra: ## Provision buckets + namespaces [Spark]
	$(call RUN_STEP,Apply infrastructure,,\
	$(SPARK_EXEC) "uv run skill-radar infra apply")

apply-infra-host: ## Provision buckets from host (skips namespaces) [sources .env]
	$(call RUN_STEP_HOST,Apply infrastructure (host),,uv run skill-radar infra apply)

infra-status: ## Check infrastructure status (host scope)
	$(SILENT)$(MAKE) validate-infra VERBOSE=$(VERBOSE)

run-infra: ## Apply + validate infrastructure [Spark]
	$(call RUN_STEP,Run infrastructure (apply + validate),,\
	$(SPARK_EXEC) "uv run skill-radar run infra")

run-infra-host: ## Apply + validate infrastructure from host [sources .env]
	$(call RUN_STEP_HOST,Run infrastructure (host),,uv run skill-radar run infra)

# ══════════════════════════════════════════════════════════════════════════
# Validation
# ══════════════════════════════════════════════════════════════════════════

.PHONY: validate-infra validate-infra-runtime validate-infra-all \
        validate-esco-landing validate-esco-bronze validate-esco-bronze-e2e \
        validate-esco-silver validate-adzuna-bronze validate-adzuna-silver \
        validate-gold validate-search-infra validate-search validate-kibana

validate-infra: ## Validate infrastructure (host scope) [sources .env]
	$(call RUN_STEP_HOST,Validate infrastructure (host),,\
	SKILLRADAR_RUNTIME_CONTEXT=host uv run skill-radar validate infra --scope host)

validate-infra-runtime: ## Validate infrastructure runtime [Spark]
	$(call RUN_STEP,Validate infrastructure (runtime),,\
	$(SPARK_EXEC) "SKILLRADAR_RUNTIME_CONTEXT=docker uv run skill-radar validate infra --scope runtime")

validate-infra-all: ## Validate all infrastructure (host + runtime) [Spark]
	$(call RUN_STEP,Validate infrastructure (all),,\
	$(SPARK_EXEC) "SKILLRADAR_RUNTIME_CONTEXT=docker uv run skill-radar validate infra --scope all")

validate-esco-landing: ## Validate ESCO landing zone (VERSION=.. ESCO_LANG=..)
	$(call RUN_STEP_HOST,Validate ESCO landing,,uv run skill-radar validate esco-landing --version $(VERSION) --lang $(ESCO_LANG))

validate-esco-bronze: ## Validate ESCO bronze tables [Spark]
	$(call RUN_STEP,Validate ESCO bronze (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar validate esco-bronze --version $(VERSION) --lang $(ESCO_LANG) $(_ENTITIES_FLAG) $(EXTRA)")

validate-esco-bronze-e2e: ## E2E bronze validation: extract + validate [Spark]
	$(call RUN_STEP,Validate ESCO bronze E2E (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar validate esco-bronze-e2e --version $(VERSION) --lang $(ESCO_LANG) $(_ENTITIES_FLAG) $(EXTRA)")

validate-esco-silver: ## Validate ESCO silver tables [Spark]
	$(call RUN_STEP,Validate ESCO silver (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar validate esco-silver --version $(VERSION) --lang $(ESCO_LANG) $(_ENTITIES_FLAG) $(EXTRA)")

validate-adzuna-bronze: ## Validate Adzuna bronze tables [Spark]
	$(call RUN_STEP,Validate Adzuna bronze (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar validate adzuna-bronze $(EXTRA)")

validate-adzuna-silver: ## Validate Adzuna silver tables [Spark]
	$(call RUN_STEP,Validate Adzuna silver (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar validate adzuna-silver $(EXTRA)")

validate-gold: ## Validate Gold tables [Spark]
	$(call RUN_STEP,Validate Gold tables (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar validate gold $(_GOLD_DATE_FLAG) --country $(GOLD_COUNTRY) --esco-version $(GOLD_ESCO_VERSION) --esco-lang $(GOLD_ESCO_LANG) $(EXTRA)")

validate-search-infra: ## Validate ES + Kibana reachable (host)
	$(call RUN_STEP_HOST,Validate search infra,,\
	uv run skill-radar validate search --ingestion-date 1970-01-01 --country _none --infra-only $(_SEARCH_ES_URL_FLAG_HOST) $(_SEARCH_KIBANA_URL_FLAG_HOST) $(EXTRA))

validate-search: ## Validate Elasticsearch indices [Spark]
	$(call RUN_STEP,Validate search indices (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar validate search $(_SEARCH_DATE_FLAG) --country $(SEARCH_COUNTRY) $(_SEARCH_DATASET_FLAG) $(_SEARCH_ES_URL_FLAG_DOCKER) $(_SEARCH_KIBANA_URL_FLAG_DOCKER) $(EXTRA)")

validate-kibana: ## Verify Kibana dashboards exist (host)
	$(call RUN_STEP_HOST,Validate Kibana dashboards,,\
	uv run skill-radar validate search --ingestion-date 1970-01-01 --country _none --infra-only $(_SEARCH_ES_URL_FLAG_HOST) $(_SEARCH_KIBANA_URL_FLAG_HOST) $(EXTRA))

# ══════════════════════════════════════════════════════════════════════════
# ESCO Pipeline
# ══════════════════════════════════════════════════════════════════════════

.PHONY: upload-esco upload-esco-local bronze-esco silver-esco \
        run-esco-bronze run-esco-silver

upload-esco: ## Upload ESCO artifact from dropzone [Spark]
	$(call RUN_STEP,Upload ESCO artifact (via dropzone in container),,\
	docker compose exec -T spark bash -lc "uv run skill-radar esco upload --version $(VERSION) --lang $(ESCO_LANG) $(EXTRA)")

upload-esco-local: ## Upload ESCO artifact from host (FILE=..)
	$(call RUN_STEP,Upload ESCO artifact (host),,\
	uv run skill-radar esco upload --version $(VERSION) --lang $(ESCO_LANG) --file $(FILE) $(EXTRA))

bronze-esco: ## Run ESCO bronze extraction [Spark]
	$(call RUN_STEP,Run ESCO bronze extraction (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar esco bronze --version $(VERSION) --lang $(ESCO_LANG) $(_ENTITIES_FLAG) $(EXTRA)")

silver-esco: ## Run ESCO silver formatting [Spark]
	$(call RUN_STEP,Run ESCO silver formatting (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar esco silver --version $(VERSION) --lang $(ESCO_LANG) $(_ENTITIES_FLAG) $(EXTRA)")

run-esco-bronze: ## Full ESCO bronze: upload → extract → validate
	$(SILENT)$(MAKE) upload-esco VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) bronze-esco VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) ENTITIES=$(ENTITIES) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) validate-esco-bronze VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) ENTITIES=$(ENTITIES) EXTRA=$(EXTRA) VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,ESCO bronze pipeline complete.)'

run-esco-silver: ## Full ESCO silver: format → validate
	$(SILENT)$(MAKE) silver-esco VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) ENTITIES=$(ENTITIES) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) validate-esco-silver VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) ENTITIES=$(ENTITIES) EXTRA=$(EXTRA) VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,ESCO silver pipeline complete.)'

# ══════════════════════════════════════════════════════════════════════════
# Adzuna Pipeline
# ══════════════════════════════════════════════════════════════════════════

.PHONY: adzuna-bronze adzuna-silver run-adzuna

adzuna-bronze: ## Run Adzuna bronze extraction [Spark]
	$(call RUN_STEP,Run Adzuna bronze extraction (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar adzuna bronze --preset $(ADZUNA_PRESET) --country $(ADZUNA_COUNTRY) $(_ADZUNA_MAX_PAGES_FLAG) $(_ADZUNA_RPP_FLAG) $(EXTRA)")

adzuna-silver: ## Run Adzuna silver formatting [Spark]
	$(call RUN_STEP,Run Adzuna silver formatting (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar adzuna silver --country $(ADZUNA_COUNTRY) $(_ADZUNA_DATE_FLAG) $(EXTRA)")

run-adzuna: ## Full Adzuna pipeline: bronze → validate → silver → validate
	$(SILENT)$(MAKE) adzuna-bronze ADZUNA_COUNTRY=$(ADZUNA_COUNTRY) ADZUNA_PRESET=$(ADZUNA_PRESET) ADZUNA_MAX_PAGES=$(ADZUNA_MAX_PAGES) ADZUNA_RESULTS_PER_PAGE=$(ADZUNA_RESULTS_PER_PAGE) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) validate-adzuna-bronze EXTRA=$(EXTRA) VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) adzuna-silver ADZUNA_COUNTRY=$(ADZUNA_COUNTRY) ADZUNA_INGESTION_DATE=$(ADZUNA_INGESTION_DATE) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) validate-adzuna-silver EXTRA=$(EXTRA) VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,Adzuna pipeline complete.)'

# ══════════════════════════════════════════════════════════════════════════
# Gold Pipeline
# ══════════════════════════════════════════════════════════════════════════

.PHONY: gold-matching gold-analytics run-gold

gold-matching: ## Run Gold matching [Spark]
	$(call RUN_STEP,Run Gold matching (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar gold matching $(_GOLD_DATE_FLAG) --country $(GOLD_COUNTRY) --esco-version $(GOLD_ESCO_VERSION) --esco-lang $(GOLD_ESCO_LANG) $(_GOLD_LIMIT_FLAG) $(EXTRA)")

gold-analytics: ## Run Gold analytics [Spark]
	$(call RUN_STEP,Run Gold analytics (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar gold analytics $(_GOLD_DATE_FLAG) --country $(GOLD_COUNTRY) --esco-version $(GOLD_ESCO_VERSION) --esco-lang $(GOLD_ESCO_LANG) $(EXTRA)")

run-gold: ## Full Gold pipeline: matching → analytics → validate
	$(SILENT)$(MAKE) gold-matching GOLD_COUNTRY=$(GOLD_COUNTRY) GOLD_INGESTION_DATE=$(GOLD_INGESTION_DATE) GOLD_ESCO_VERSION=$(GOLD_ESCO_VERSION) GOLD_ESCO_LANG=$(GOLD_ESCO_LANG) GOLD_JOB_LIMIT=$(GOLD_JOB_LIMIT) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) gold-analytics GOLD_COUNTRY=$(GOLD_COUNTRY) GOLD_INGESTION_DATE=$(GOLD_INGESTION_DATE) GOLD_ESCO_VERSION=$(GOLD_ESCO_VERSION) GOLD_ESCO_LANG=$(GOLD_ESCO_LANG) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) validate-gold GOLD_COUNTRY=$(GOLD_COUNTRY) GOLD_INGESTION_DATE=$(GOLD_INGESTION_DATE) GOLD_ESCO_VERSION=$(GOLD_ESCO_VERSION) GOLD_ESCO_LANG=$(GOLD_ESCO_LANG) EXTRA=$(EXTRA) VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,Gold pipeline complete.)'

# ══════════════════════════════════════════════════════════════════════════
# Search (Elasticsearch + Kibana)
# ══════════════════════════════════════════════════════════════════════════

.PHONY: search-up search-down search-reset search-logs \
        export-search bootstrap-kibana export-kibana-assets apply-kibana-assets \
        run-search wait-search-healthy

search-up: ## Start Elasticsearch + Kibana
	$(call RUN_STEP,Start search stack (ES + Kibana),,$(SEARCH_COMPOSE) up -d)
	$(SILENT)bash -lc '$(call UI_OK,Search stack running: ES=http://localhost:$${ES_PORT:-9200} Kibana=http://localhost:$${KIBANA_PORT:-5601})'

search-down: ## Stop Elasticsearch + Kibana
	$(call RUN_STEP,Stop search stack,,$(SEARCH_COMPOSE) down)

search-reset: ## Stop search stack and remove data volumes
	$(call RUN_STEP,Reset search stack,,$(SEARCH_COMPOSE) down -v)

search-logs: ## Tail ES + Kibana logs
	@$(SEARCH_COMPOSE) logs -f --tail=200 elasticsearch kibana

export-search: ## Export Gold data to Elasticsearch [Spark]
	$(call RUN_STEP,Export to Elasticsearch (via Spark),,\
	$(SPARK_EXEC) "uv run skill-radar search export $(_SEARCH_DATE_FLAG) --country $(SEARCH_COUNTRY) $(_SEARCH_DATASET_FLAG) $(_SEARCH_ES_URL_FLAG_DOCKER) --alias-swap --refresh $(EXTRA)")

bootstrap-kibana: ## Create Kibana data views + dashboards
	$(call RUN_STEP_HOST,Bootstrap Kibana data views + dashboards,,\
	uv run skill-radar search dashboard apply $(_SEARCH_KIBANA_URL_FLAG_HOST) --overwrite $(EXTRA))

export-kibana-assets: ## Generate NDJSON artifact only (no push)
	$(call RUN_STEP_HOST,Export Kibana dashboard assets to NDJSON,,\
	uv run skill-radar search dashboard export $(EXTRA))

apply-kibana-assets: ## Push generated dashboard assets to Kibana
	$(call RUN_STEP_HOST,Apply Kibana dashboard assets,,\
	uv run skill-radar search dashboard apply $(_SEARCH_KIBANA_URL_FLAG_HOST) --overwrite $(EXTRA))

run-search: ## Full search pipeline: export → kibana → validate
	$(SILENT)$(MAKE) export-search SEARCH_COUNTRY=$(SEARCH_COUNTRY) SEARCH_INGESTION_DATE=$(SEARCH_INGESTION_DATE) SEARCH_DATASET=$(SEARCH_DATASET) SEARCH_ES_URL=$(SEARCH_ES_URL) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) bootstrap-kibana SEARCH_KIBANA_URL=$(SEARCH_KIBANA_URL) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) validate-search SEARCH_COUNTRY=$(SEARCH_COUNTRY) SEARCH_INGESTION_DATE=$(SEARCH_INGESTION_DATE) SEARCH_DATASET=$(SEARCH_DATASET) SEARCH_ES_URL=$(SEARCH_ES_URL) SEARCH_KIBANA_URL=$(SEARCH_KIBANA_URL) EXTRA= VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,Search pipeline complete.)'

wait-search-healthy: ## Wait for ES + Kibana to be healthy
	$(SILENT)bash -lc '\
	$(call UI_INFO,Waiting for Elasticsearch...); \
	for i in $$(seq 1 60); do \
	  if curl -fsS http://localhost:$${ES_PORT:-9200}/_cluster/health >/dev/null 2>&1; then \
	    $(call UI_OK,Elasticsearch is healthy); break; \
	  fi; \
	  if [ $$i -eq 60 ]; then $(call UI_FAIL,Elasticsearch did not become healthy); exit 1; fi; \
	  sleep 2; \
	done; \
	$(call UI_INFO,Waiting for Kibana...); \
	for i in $$(seq 1 60); do \
	  if curl -fsS http://localhost:$${KIBANA_PORT:-5601}/api/status >/dev/null 2>&1; then \
	    $(call UI_OK,Kibana is healthy); break; \
	  fi; \
	  if [ $$i -eq 60 ]; then $(call UI_FAIL,Kibana did not become healthy); exit 1; fi; \
	  sleep 2; \
	done'

# ══════════════════════════════════════════════════════════════════════════
# Airflow
# ══════════════════════════════════════════════════════════════════════════

.PHONY: airflow-up airflow-down airflow-reset airflow-logs \
        airflow-dags-list airflow-trigger-adzuna airflow-trigger-esco \
        airflow-test-adzuna airflow-test-esco airflow-smoke

airflow-up: ## Start Airflow (scheduler + webserver)
	$(call RUN_STEP,Build Airflow image,,$(AIRFLOW_COMPOSE) build airflow-init)
	$(call RUN_STEP,Start Airflow stack,,$(AIRFLOW_COMPOSE) up -d)
	$(SILENT)bash -lc '$(call UI_OK,Airflow running at http://localhost:$${AIRFLOW_WEB_PORT:-8085})'

airflow-down: ## Stop Airflow
	$(call RUN_STEP,Stop Airflow stack,,$(AIRFLOW_COMPOSE) down)

airflow-reset: ## Stop Airflow and remove volumes
	$(call RUN_STEP,Reset Airflow stack,,$(AIRFLOW_COMPOSE) down -v)

airflow-logs: ## Tail Airflow logs
	@$(AIRFLOW_COMPOSE) logs -f --tail=200 airflow-scheduler airflow-webserver

airflow-dags-list: ## List discovered DAGs
	$(call RUN_STEP,List Airflow DAGs,,$(AIRFLOW_EXEC) airflow dags list)

airflow-trigger-adzuna: ## Trigger Adzuna daily DAG (AIRFLOW_ADZUNA_DATE=..)
	$(call RUN_STEP,Trigger adzuna_daily_pipeline ($(AIRFLOW_ADZUNA_DATE)),,\
	$(AIRFLOW_EXEC) airflow dags trigger adzuna_daily_pipeline -e $(AIRFLOW_ADZUNA_DATE))

airflow-trigger-esco: ## Trigger ESCO manual DAG (AIRFLOW_ESCO_VERSION=.. AIRFLOW_ESCO_LANG=..)
	@printf "$(C_BLUE)▶$(C_RESET) %s\n" "Trigger esco_manual_pipeline ($(AIRFLOW_ESCO_VERSION)/$(AIRFLOW_ESCO_LANG))"
	$(SILENT)$(AIRFLOW_EXEC) \
		airflow dags trigger esco_manual_pipeline \
		--conf '{"version": "$(AIRFLOW_ESCO_VERSION)", "lang": "$(AIRFLOW_ESCO_LANG)", "run_gold_after": $(AIRFLOW_ESCO_RUN_GOLD)}'
	@printf "$(C_GREEN)✓$(C_RESET) %s\n" "esco_manual_pipeline triggered"

airflow-test-adzuna: ## Validate Adzuna DAG integrity
	$(call RUN_STEP,Check DAG import errors,,$(AIRFLOW_EXEC) airflow dags list-import-errors)
	$(call RUN_STEP,Validate adzuna_daily_pipeline task tree,,$(AIRFLOW_EXEC) airflow tasks list adzuna_daily_pipeline --tree)

airflow-test-esco: ## Validate ESCO DAG integrity
	$(call RUN_STEP,Check DAG import errors,,$(AIRFLOW_EXEC) airflow dags list-import-errors)
	$(call RUN_STEP,Validate esco_manual_pipeline task tree,,$(AIRFLOW_EXEC) airflow tasks list esco_manual_pipeline --tree)

airflow-smoke: ## Verify DockerOperator prerequisites
	$(call RUN_STEP,DockerOperator smoke check,,\
	$(AIRFLOW_EXEC) python /opt/airflow/scripts/docker_operator_smoke.py)

# ══════════════════════════════════════════════════════════════════════════
# Grouped / End-to-End
# ══════════════════════════════════════════════════════════════════════════

.PHONY: ci dev nuke up-all run-all

ci: ## Run full CI pipeline (all local checks)
	$(SILENT)$(MAKE) check VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,CI pipeline passed.)'

dev: ## Quality checks + infra health
	$(SILENT)$(MAKE) quality VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) infra VERBOSE=$(VERBOSE)
	$(SILENT)bash -lc '$(call UI_OK,Dev workflow passed.)'

nuke: ## Factory reset: stop ALL services, remove ALL volumes, clean logs
	$(SILENT)bash -lc '$(call UI_WARN,NUKE — Destroying all containers + volumes + logs)'
	$(call RUN_STEP,Stop all containers + remove volumes,,$(ALL_PROFILES) down -v --remove-orphans)
	$(call RUN_STEP,Clean local logs,,rm -rf logs/validation logs/*.log)
	$(SILENT)bash -lc '$(call UI_OK,Everything destroyed. Run make up-all to start fresh.)'

up-all: ## Start ALL services (core + Airflow + search)
	$(SILENT)bash -lc '$(call UI_INFO,Starting ALL Skill Radar services)'
	$(call RUN_STEP,Build Spark image,,docker compose build spark)
	$(call RUN_STEP,Build Airflow image,,$(ALL_PROFILES) build airflow-init)
	$(call RUN_STEP,Start all services,,$(ALL_PROFILES) up -d)
	$(call RUN_STEP,Wait for ES + Kibana,,$(MAKE) wait-search-healthy VERBOSE=$(VERBOSE))
	$(call RUN_STEP,Spark + Iceberg smoke test,,$(MAKE) smoke VERBOSE=$(VERBOSE))
	$(SILENT)bash -lc '$(call UI_OK,All services running.)'

run-all: ## Full pipeline: infra → ESCO → Adzuna → Gold → Search → Kibana
	$(SILENT)bash -lc '$(call UI_INFO,Skill Radar — Full Pipeline ($(PIPELINE_DATE) / $(PIPELINE_COUNTRY)))'
	@echo ""
	$(SILENT)bash -lc '$(call UI_INFO,[1/7] Infrastructure provisioning...)'
	$(SILENT)$(MAKE) run-infra VERBOSE=$(VERBOSE)
	@echo ""
	$(SILENT)bash -lc '$(call UI_INFO,[2/7] Starting search stack...)'
	$(SILENT)$(MAKE) search-up VERBOSE=$(VERBOSE)
	@echo ""
	$(SILENT)bash -lc '$(call UI_INFO,[3/7] ESCO pipeline (upload → bronze → silver)...)'
	$(SILENT)$(MAKE) upload-esco VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) EXTRA=--force VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) bronze-esco VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) ENTITIES=$(ENTITIES) VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) validate-esco-bronze VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) ENTITIES=$(ENTITIES) VERBOSE=$(VERBOSE)
	$(SILENT)$(MAKE) run-esco-silver VERSION=$(VERSION) ESCO_LANG=$(ESCO_LANG) ENTITIES=$(ENTITIES) VERBOSE=$(VERBOSE)
	@echo ""
	$(SILENT)bash -lc '$(call UI_INFO,[4/7] Adzuna pipeline (bronze → silver)...)'
	$(SILENT)$(MAKE) run-adzuna \
		ADZUNA_COUNTRY=$(PIPELINE_COUNTRY) \
		ADZUNA_INGESTION_DATE=$(PIPELINE_DATE) \
		ADZUNA_PRESET=$(ADZUNA_PRESET) \
		ADZUNA_MAX_PAGES=$(ADZUNA_MAX_PAGES) \
		ADZUNA_RESULTS_PER_PAGE=$(ADZUNA_RESULTS_PER_PAGE) \
		VERBOSE=$(VERBOSE)
	@echo ""
	$(SILENT)bash -lc '$(call UI_INFO,[5/7] Gold pipeline (matching → analytics → validate)...)'
	$(SILENT)$(MAKE) run-gold \
		GOLD_COUNTRY=$(PIPELINE_COUNTRY) \
		GOLD_INGESTION_DATE=$(PIPELINE_DATE) \
		GOLD_ESCO_VERSION=$(VERSION) \
		GOLD_ESCO_LANG=$(ESCO_LANG) \
		GOLD_JOB_LIMIT=$(GOLD_JOB_LIMIT) \
		VERBOSE=$(VERBOSE)
	@echo ""
	$(SILENT)bash -lc '$(call UI_INFO,[6/7] Waiting for search stack...)'
	$(SILENT)$(MAKE) wait-search-healthy VERBOSE=$(VERBOSE)
	@echo ""
	$(SILENT)bash -lc '$(call UI_INFO,[7/7] Search pipeline (export → validate → Kibana)...)'
	$(SILENT)$(MAKE) run-search \
		SEARCH_COUNTRY=$(PIPELINE_COUNTRY) \
		SEARCH_INGESTION_DATE=$(PIPELINE_DATE) \
		SEARCH_ES_URL=$(SEARCH_ES_URL) \
		SEARCH_KIBANA_URL=$(SEARCH_KIBANA_URL) \
		VERBOSE=$(VERBOSE)
	@echo ""
	$(SILENT)bash -lc '$(call UI_OK,Full pipeline complete! Kibana: http://localhost:$${KIBANA_PORT:-5601})'
