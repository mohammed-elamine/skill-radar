#!/usr/bin/env bash
set -euo pipefail

# ----------------------------
# Skill Radar: doctor
# ----------------------------

usage() {
  cat <<'EOF'
Usage: ./scripts/doctor.sh [--fix] [--live]

Options:
  --fix   Attempt to fix common setup issues:
          - run "uv sync --dev"
          - install pre-commit hooks if missing
  --live  Perform a live Adzuna API request (requires ADZUNA_APP_ID/KEY).
          Default is OFF to avoid consuming quota and leaking data in logs.
EOF
}

FIX=0
LIVE=0

for arg in "$@"; do
  case "$arg" in
    --fix) FIX=1 ;;
    --live) LIVE=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $arg"; usage; exit 1 ;;
  esac
done

echo "== Skill Radar: doctor =="

# 1) Tooling checks
if ! command -v uv >/dev/null 2>&1; then
  echo "❌ uv not found."
  echo "Install: https://astral.sh/uv"
  exit 1
fi
echo "✅ uv found: $(uv --version)"

# 2) Ensure deps + hooks (optional)
if [[ "$FIX" -eq 1 ]]; then
  echo "== Applying fixes (--fix) =="
  echo "→ Installing dependencies with uv..."
  uv sync --dev
fi

# 3) Python version check (via uv environment)
PYVER="$(uv run python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
if [[ "$PYVER" != "3.11" ]]; then
  echo "❌ Python $PYVER detected. Expected 3.11."
  echo "If you manage Python versions (pyenv/asdf), switch to 3.11 and re-run."
  exit 1
fi
echo "✅ Python version OK: $PYVER"

# 4) .env existence + load
if [[ ! -f ".env" ]]; then
  echo "⚠️  .env not found."
  echo "Create it from .env.example:"
  echo "    cp .env.example .env"
else
  # shellcheck disable=SC1091
  source .env
  echo "✅ .env loaded"
fi

# 5) Required env vars
missing=0
for var in ADZUNA_APP_ID ADZUNA_APP_KEY; do
  if [[ -z "${!var:-}" ]]; then
    echo "❌ Missing env var: $var"
    missing=1
  else
    echo "✅ Found env var: $var"
  fi
done

if [[ "$missing" -eq 1 ]]; then
  echo "Fix missing variables in .env and re-run."
  exit 1
fi

# 6) pre-commit hook presence (and install if --fix)
HOOK_PATH=".git/hooks/pre-commit"
if [[ -f "$HOOK_PATH" ]]; then
  echo "✅ pre-commit hook installed ($HOOK_PATH)"
else
  echo "⚠️  pre-commit hook not installed."
  echo "Install with:"
  echo "    uv run pre-commit install"

  if [[ "$FIX" -eq 1 ]]; then
    echo "→ Installing pre-commit hook..."
    uv run pre-commit install
    echo "✅ pre-commit hook installed"
  fi
fi

# 7) Import check
echo "== Import check =="
uv run python -c "import skill_radar; print('✅ skill_radar import OK')"

# 8) Data directory checks
DATA_DIR="${SKILL_RADAR_DATA_DIR:-./data}"
mkdir -p "$DATA_DIR" >/dev/null 2>&1 || {
  echo "❌ Cannot create data directory: $DATA_DIR"
  exit 1
}
touch "$DATA_DIR/.doctor_write_test" 2>/dev/null || {
  echo "❌ Cannot write to data directory: $DATA_DIR"
  exit 1
}
rm -f "$DATA_DIR/.doctor_write_test"
echo "✅ Data directory OK (exists & writable): $DATA_DIR"

# 9) Java/Spark check (only if pyspark is installed, as it's an optional dependency)
if uv run python -c "import importlib; import sys; sys.exit(0 if importlib.util.find_spec('pyspark') else 1)" >/dev/null 2>&1; then
  echo "== Spark prerequisite check =="
  if ! command -v java >/dev/null 2>&1; then
    echo "❌ Java not found, but pyspark is installed."
    echo "Install Java 17+ (Spark 3.5 requires it)."
    exit 1
  fi

  JAVA_VER_RAW="$(java -version 2>&1 | head -n 1)"
  echo "java -version: $JAVA_VER_RAW"

  # Extract major version (handles formats like 'openjdk version "17.0.10"' or 'java version "17.0.2"')
  JAVA_MAJOR="$(java -version 2>&1 | head -n 1 | sed -E 's/.*version "([0-9]+).*/\1/')"
  if [[ -z "${JAVA_MAJOR:-}" ]]; then
    echo "⚠️  Could not parse Java version. Ensure Java 17+ is installed."
    exit 1
  fi

  if (( JAVA_MAJOR < 17 )); then
    echo "❌ Java $JAVA_MAJOR detected. Java 17+ is required for Spark 3.5."
    exit 1
  fi

  echo "✅ Java requirement OK: $JAVA_MAJOR"
else
  echo "pyspark not installed; skipping Java check."
fi

# 10) Optional live API check
if [[ "$LIVE" -eq 1 ]]; then
  echo "== Live API check (--live) =="
  # Minimal request to avoid large payloads; do not print response content
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
