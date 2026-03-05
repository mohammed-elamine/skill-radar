# docker/spark/Dockerfile
# -----------------------------------------------------------------------------
# Spark runtime image for SkillRadar
#
# Goals:
# - Spark 3.5.7 base image
# - Java 17 for Spark runtime (force, despite base image shipping Java 11)
# - Python 3.11 (driver/executor)
# - uv-managed virtualenv (uv.lock) for deterministic Python deps
# - Pre-baked Iceberg + Hadoop AWS + AWS SDK jars (no Ivy resolution at runtime)
# - Baked Spark config (spark-defaults.conf + log4j2.properties)
#
# Notes:
# - The upstream spark:3.5.7-python3 image includes Java 11 in /opt/java/openjdk.
#   We install Temurin 17 and *force* Spark to use it via JAVA_HOME + PATH and
#   spark-env.sh. We never validate versions via pipes to avoid SIGPIPE (exit 141).
# -----------------------------------------------------------------------------

FROM spark:3.5.7-python3

USER root
SHELL ["/bin/bash", "-lc"]

# -----------------------------------------------------------------------------
# 0) Base OS tools
# -----------------------------------------------------------------------------
RUN set -euxo pipefail; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      gnupg \
      software-properties-common \
      wget; \
    rm -rf /var/lib/apt/lists/*

# -----------------------------------------------------------------------------
# 1) Java 17 (Temurin) — force correct JVM despite base image Java 11
# -----------------------------------------------------------------------------
RUN set -euxo pipefail; \
    mkdir -p /etc/apt/keyrings; \
    wget -qO- https://packages.adoptium.net/artifactory/api/gpg/key/public \
      | gpg --dearmor > /etc/apt/keyrings/adoptium.gpg; \
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb jammy main" \
      > /etc/apt/sources.list.d/adoptium.list; \
    apt-get update; \
    apt-get install -y --no-install-recommends temurin-17-jdk; \
    rm -rf /var/lib/apt/lists/*; \
    \
    # Resolve the real Temurin 17 home from package contents (amd64/arm64 safe).
    java17_bin="$(dpkg -L temurin-17-jdk | awk '/\/bin\/java$/{print; exit}')"; \
    test -n "$java17_bin"; \
    test -x "$java17_bin"; \
    java17_home="$(dirname "$(dirname "$java17_bin")")"; \
    test -x "$java17_home/bin/javac"; \
    \
    echo "Detected JAVA17_HOME=$java17_home"; \
    mkdir -p /usr/lib/jvm; \
    ln -sfn "$java17_home" /usr/lib/jvm/temurin-17; \
    \
    # Verify using capture (no SIGPIPE risk => no exit 141).
    out="$(/usr/lib/jvm/temurin-17/bin/java -version 2>&1)"; \
    echo "$out"; \
    grep -Eq 'version "17\.' <<< "$out"

ENV JAVA_HOME=/usr/lib/jvm/temurin-17
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# -----------------------------------------------------------------------------
# 2) SPARK_HOME (pinned for this base image)
# -----------------------------------------------------------------------------
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN set -euxo pipefail; \
    test -d "$SPARK_HOME"; \
    test -x "$SPARK_HOME/bin/spark-submit"; \
    "$SPARK_HOME/bin/spark-submit" --version

# -----------------------------------------------------------------------------
# 3) Python 3.11
# -----------------------------------------------------------------------------
# Base image ships with Python 3.10; project requires 3.11.
RUN set -euxo pipefail; \
    apt-get update; \
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

# -----------------------------------------------------------------------------
# 4) uv (system-wide)
# -----------------------------------------------------------------------------
RUN set -euxo pipefail; \
    curl -LsSf https://astral.sh/uv/install.sh | sh; \
    ln -sf /root/.local/bin/uv /usr/local/bin/uv; \
    uv --version

# -----------------------------------------------------------------------------
# 5) Pre-bake Spark runtime JARs (Iceberg + Hadoop AWS + AWS SDK bundle)
# -----------------------------------------------------------------------------
COPY docker/spark/fetch-jars.sh /tmp/fetch-jars.sh
RUN set -euxo pipefail; \
    chmod +x /tmp/fetch-jars.sh; \
    /tmp/fetch-jars.sh "${SPARK_HOME}/jars"

# -----------------------------------------------------------------------------
# 6) Spark config (baked-in defaults; compose can still override with mounts)
# -----------------------------------------------------------------------------
COPY configs/spark-defaults.conf "${SPARK_HOME}/conf/spark-defaults.conf"
COPY configs/log4j2.properties   "${SPARK_HOME}/conf/log4j2.properties"

# -----------------------------------------------------------------------------
# 7) Project setup (uv venv)
# -----------------------------------------------------------------------------
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

# -----------------------------------------------------------------------------
# 8) spark-env.sh: force Java/Python for Spark runtime (no heredoc pitfalls)
# -----------------------------------------------------------------------------
RUN set -euxo pipefail; \
    mkdir -p "${SPARK_HOME}/conf"; \
    printf '%s\n' \
      "export JAVA_HOME=/usr/lib/jvm/temurin-17" \
      'export PATH="$JAVA_HOME/bin:$PATH"' \
      '' \
      '# Ensure driver/executors use the project venv python' \
      'export PYSPARK_PYTHON=/opt/skillradar/.venv/bin/python' \
      'export PYSPARK_DRIVER_PYTHON=/opt/skillradar/.venv/bin/python' \
      > "${SPARK_HOME}/conf/spark-env.sh"; \
    chmod 0644 "${SPARK_HOME}/conf/spark-env.sh"; \
    echo "Wrote ${SPARK_HOME}/conf/spark-env.sh"; \
    cat "${SPARK_HOME}/conf/spark-env.sh"; \
    \
    # Final sanity: validate *directly* (no pipes).
    out="$("${JAVA_HOME}/bin/java" -version 2>&1)"; \
    echo "$out"; \
    grep -Eq 'version "17\.' <<< "$out"; \
    python --version; \
    python -c "import pyspark; print('pyspark:', pyspark.__version__)"

# -----------------------------------------------------------------------------
# 9) Healthcheck
# -----------------------------------------------------------------------------
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python --version && uv --version

CMD ["bash"]
