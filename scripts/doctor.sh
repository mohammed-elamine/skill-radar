#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./scripts/doctor.sh [--fix] [--live] [--infra-only]

Options:
  --fix        Attempt to fix common setup issues:
               - uv sync --dev
               - pre-commit install
  --live       Perform a live Adzuna API request (requires ADZUNA_APP_ID/KEY).
  --infra-only Only check docker-compose infrastructure and smoke test.
EOF
}

FIX=0
LIVE=0
INFRA_ONLY=0

for arg in "$@"; do
  case "$arg" in
    --fix) FIX=1 ;;
    --live) LIVE=1 ;;
    --infra-only) INFRA_ONLY=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $arg"; usage; exit 1 ;;
  esac
done

echo "== Skill Radar: doctor =="

if [[ "$INFRA_ONLY" -eq 0 ]]; then
  # 1) Tooling checks
  if ! command -v uv >/dev/null 2>&1; then
    echo "❌ uv not found."
    echo "Install: https://astral.sh/uv"
    exit 1
  fi
  echo "✅ uv found: $(uv --version)"

  # 2) Ensure deps (optional)
  if [[ "$FIX" -eq 1 ]]; then
    echo "== Applying fixes (--fix) =="
    uv sync --dev
  fi

  # 3) Python check (project runtime)
  PYVER="$(uv run python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
  echo "✅ Python detected via uv: $PYVER"

  # 4) .env existence
  if [[ ! -f ".env" ]]; then
    echo "⚠️  .env not found. Create it from .env.example:"
    echo "    cp .env.example .env"
  else
    echo "✅ .env found"
  fi

  # 5) pre-commit hook presence (install if --fix)
  HOOK_PATH=".git/hooks/pre-commit"
  if [[ -f "$HOOK_PATH" ]]; then
    echo "✅ pre-commit hook installed"
  else
    echo "⚠️  pre-commit hook not installed."
    if [[ "$FIX" -eq 1 ]]; then
      uv run pre-commit install
      echo "✅ pre-commit hook installed"
    else
      echo "Install with: uv run pre-commit install"
    fi
  fi

  # 6) Import check
  echo "== Import check =="
  uv run python -c "import skill_radar; print('✅ skill_radar import OK')"
fi

# ----------------------------
# Infrastructure checks (docker-compose)
# ----------------------------
echo "== Docker infrastructure check =="

if ! command -v docker >/dev/null 2>&1; then
  echo "❌ docker not found."
  exit 1
fi
echo "✅ docker found: $(docker --version)"

if ! docker compose version >/dev/null 2>&1; then
  echo "❌ docker compose not available."
  exit 1
fi
echo "✅ docker compose available"

echo "== docker compose ps =="
docker compose ps

# MinIO readiness (best-effort)
MINIO_PORT="${MINIO_PORT:-9000}"
if command -v curl >/dev/null 2>&1; then
  echo "== MinIO health check =="
  curl -fsS "http://127.0.0.1:${MINIO_PORT}/minio/health/ready" >/dev/null \
    && echo "✅ MinIO is ready" \
    || echo "⚠️  MinIO health endpoint not reachable (is the stack up?)"
else
  echo "⚠️  curl not available; skipping MinIO health check"
fi

# Spark + Iceberg smoke test
echo "== Spark + Iceberg smoke test =="
docker compose exec -T spark bash -lc "spark-submit /opt/skillradar/jobs/examples/iceberg_smoke_test.py"
echo "✅ Smoke test OK"

# ----------------------------
# Optional live API check
# ----------------------------
if [[ "$LIVE" -eq 1 ]]; then
  echo "== Live Adzuna API check (--live) =="

  # shellcheck disable=SC1091
  [[ -f ".env" ]] && source .env || true

  if [[ -z "${ADZUNA_APP_ID:-}" || -z "${ADZUNA_APP_KEY:-}" ]]; then
    echo "❌ ADZUNA_APP_ID / ADZUNA_APP_KEY missing in environment."
    exit 1
  fi

  STATUS="$(uv run python - <<'PY'
import os, requests
country="fr"
url=f"https://api.adzuna.com/v1/api/jobs/{country}/search/1"
params={
  "app_id": os.environ["ADZUNA_APP_ID"],
  "app_key": os.environ["ADZUNA_APP_KEY"],
  "results_per_page": 1,
  "what": "data"
}
r=requests.get(url, params=params, timeout=20)
print(r.status_code)
PY
)"
  if [[ "$STATUS" == "200" ]]; then
    echo "✅ Live Adzuna request OK (HTTP 200)"
  else
    echo "❌ Live Adzuna request failed (HTTP $STATUS)"
    exit 1
  fi
fi

echo "✅ Doctor check passed."
