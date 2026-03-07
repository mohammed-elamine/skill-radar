# docker/spark/Dockerfile
# -----------------------------------------------------------------------------
# Spark runtime image for SkillRadar
#
# Base: Official Apache Spark image with Scala 2.12, Java 11, Python 3,
#       on Ubuntu 22.04.  Java 11 is bundled and tested by the upstream
#       Spark release — no manual JDK installation required.
#
# Rationale:
# - Spark 3.5 officially supports Java 11 (the primary test target).
# - Java 11 is stable on both amd64 and aarch64 in Docker Desktop.
# - Using the official image avoids JDK-17 aarch64 runtime bugs
#   (symbol.cpp refcount underflow — SIGSEGV in ConcurrentHashTable).
#
# Layers:
#   0) OS tools (curl, software-properties-common for deadsnakes PPA)
#   1) Python 3.11 (project requirement; base ships 3.10)
#   2) uv package manager
#   3) Spark runtime JARs (Iceberg + Hadoop AWS + AWS SDK)
#   4) Spark configuration
#   5) Project dependencies & source
#   6) spark-env.sh & final sanity checks
# -----------------------------------------------------------------------------

FROM spark:3.5.7-scala2.12-java11-python3-ubuntu

USER root
SHELL ["/bin/bash", "-lc"]

# ── 0) OS tools ─────────────────────────────────────────────────────────────
RUN set -euxo pipefail; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      gnupg \
      software-properties-common \
      wget; \
    rm -rf /var/lib/apt/lists/*

# ── 1) Python 3.11 ─────────────────────────────────────────────────────────
# Base ships Python 3.10; project requires 3.11.
RUN set -euxo pipefail; \
    add-apt-repository -y ppa:deadsnakes/ppa; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      python3.11 \
      python3.11-dev \
      python3.11-venv; \
    rm -rf /var/lib/apt/lists/*; \
    update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1; \
    update-alternatives --set python3 /usr/bin/python3.11; \
    python3 --version

# ── 2) uv (system-wide) ────────────────────────────────────────────────────
RUN set -euxo pipefail; \
    curl -LsSf https://astral.sh/uv/install.sh | sh; \
    ln -sf /root/.local/bin/uv /usr/local/bin/uv; \
    uv --version

# ── 3) Pre-bake Spark runtime JARs (Iceberg + Hadoop AWS + AWS SDK) ────────
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

COPY docker/spark/fetch-jars.sh /tmp/fetch-jars.sh
RUN set -euxo pipefail; \
    chmod +x /tmp/fetch-jars.sh; \
    /tmp/fetch-jars.sh "${SPARK_HOME}/jars"

# ── 4) Spark configuration ─────────────────────────────────────────────────
RUN mkdir -p "${SPARK_HOME}/conf"
COPY configs/spark-defaults.conf "${SPARK_HOME}/conf/spark-defaults.conf"
COPY configs/log4j2.properties   "${SPARK_HOME}/conf/log4j2.properties"

# ── 5) Project setup (uv venv) ─────────────────────────────────────────────
WORKDIR /opt/skillradar

# Dependency metadata first for layer caching
COPY pyproject.toml uv.lock README.md ./

# Source code
COPY src/  ./src/
COPY jobs/ ./jobs/

ENV UV_PYTHON=python3.11
ENV UV_PROJECT_ENVIRONMENT=/opt/skillradar/.venv

RUN set -euxo pipefail; \
    uv sync --frozen --no-dev

ENV PATH="/opt/skillradar/.venv/bin:${PATH}"
ENV PYSPARK_PYTHON=/opt/skillradar/.venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/skillradar/.venv/bin/python
ENV PYTHONPATH=/opt/skillradar/src

# ── 6) spark-env.sh: Python for Spark runtime ──────────────────────────────
# JAVA_HOME is already set correctly by the base image (/opt/java/openjdk).
# We only need to point Spark at the project venv Python.
RUN set -euxo pipefail; \
    printf '%s\n' \
      '# Ensure driver/executors use the project venv python' \
      'export PYSPARK_PYTHON=/opt/skillradar/.venv/bin/python' \
      'export PYSPARK_DRIVER_PYTHON=/opt/skillradar/.venv/bin/python' \
      > "${SPARK_HOME}/conf/spark-env.sh"; \
    chmod 0644 "${SPARK_HOME}/conf/spark-env.sh"; \
    \
    # Final sanity checks (no pipes — no SIGPIPE risk).
    out="$(java -version 2>&1)"; echo "$out"; \
    python --version; \
    python -c "import pyspark; print('pyspark:', pyspark.__version__)"

# ── 7) Healthcheck ─────────────────────────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python --version && uv --version

CMD ["bash"]
