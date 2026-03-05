#!/usr/bin/env bash
# docker/spark/fetch-jars.sh
#
# Download Spark runtime jars at build time to eliminate Ivy resolution spam.
# Each jar is verified against a known SHA-256 checksum.
#
# Usage: ./fetch-jars.sh <target-dir>
#   e.g. ./fetch-jars.sh /opt/spark/jars
#
# Versions (pinned for reproducibility):
#   - Iceberg Spark Runtime: 1.5.2
#   - Hadoop AWS: 3.3.4
#   - AWS SDK Bundle: 1.12.262

set -euo pipefail

TARGET_DIR="${1:-/opt/spark/jars}"

# ============================================================================
# JAR DEFINITIONS: name, URL, SHA-256
# ============================================================================
# Format: "filename|url|sha256"
JARS=(
  "iceberg-spark-runtime-3.5_2.12-1.5.2.jar|https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar|2aec4b78d806cc81d4e50325b351f6349c90de641544e644c6032f000938618b"
  "hadoop-aws-3.3.4.jar|https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar|53f9ae03c681a30a50aa17524bd9790ab596b28481858e54efd989a826ed3a4a"
  "aws-java-sdk-bundle-1.12.262.jar|https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar|873fe7cf495126619997bec21c44de5d992544aea7e632fdc77adb1a0915bae5"
)

# ============================================================================
# DOWNLOAD & VERIFY
# ============================================================================
echo "==> Downloading Spark runtime jars to ${TARGET_DIR}"

for entry in "${JARS[@]}"; do
  IFS='|' read -r filename url expected_sha256 <<< "$entry"
  dest="${TARGET_DIR}/${filename}"

  if [[ -f "$dest" ]]; then
    echo "    [SKIP] ${filename} already exists"
    continue
  fi

  echo "    [GET]  ${filename}"
  curl -fsSL -o "$dest" "$url"

  # Compute SHA-256
  if command -v sha256sum &>/dev/null; then
    actual_sha256=$(sha256sum "$dest" | awk '{print $1}')
  elif command -v shasum &>/dev/null; then
    actual_sha256=$(shasum -a 256 "$dest" | awk '{print $1}')
  else
    echo "ERROR: No sha256sum or shasum available" >&2
    exit 1
  fi

  if [[ "$actual_sha256" != "$expected_sha256" ]]; then
    echo "ERROR: Checksum mismatch for ${filename}" >&2
    echo "       Expected: ${expected_sha256}" >&2
    echo "       Got:      ${actual_sha256}" >&2
    rm -f "$dest"
    exit 1
  fi

  echo "    [OK]   ${filename} (sha256 verified)"
done

echo "==> All jars downloaded and verified successfully"
