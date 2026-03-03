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
