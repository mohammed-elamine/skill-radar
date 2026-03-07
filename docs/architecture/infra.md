# Infrastructure (Local, Production-Aligned)

This project uses a **local-first lakehouse stack** that mirrors production patterns:
- **Object storage**: MinIO (S3-compatible)
- **Table format**: Apache Iceberg (ACID tables, schema evolution, snapshots)
- **Compute**: Apache Spark 3.5.7 (batch processing, JDK 11)
- **Orchestration**: Apache Airflow (DAGs launch Spark via DockerOperator)
- (Optional later) **Serving/search**: Elasticsearch + Kibana

The goal of this infrastructure milestone is to guarantee that:
1. Spark can connect to the S3-compatible storage (MinIO) using S3A.
2. Spark can read/write **Iceberg tables** in that storage.
3. The team has a repeatable developer workflow and a minimal test harness.

---

## Components and roles

### MinIO
- Provides an S3-compatible API locally.
- Stores:
  - Iceberg **warehouse** (table metadata and data files)
  - Raw/curated lake paths under `data/...` (when you implement Bronze/Silver/Gold)

### Spark
- Runs ingestion and transformation jobs.
- Official base image: `spark:3.5.7-scala2.12-java11-python3-ubuntu`
- Uses:
  - S3A connector (Hadoop AWS) to read/write to MinIO
  - Iceberg Spark runtime to manage Iceberg tables and namespaces
  - Eclipse Temurin JDK 11 (bundled by the base image)

### Iceberg
- Defines table format and transaction semantics.
- Provides:
  - Atomic commits
  - Snapshot history
  - Schema evolution
  - Partition evolution

---

## Spark Runtime

### Base image

The project uses the **official Apache Spark Docker image** as its base:

```
spark:3.5.7-scala2.12-java11-python3-ubuntu
```

This image ships Spark 3.5.7, Scala 2.12, and **Eclipse Temurin JDK 11** on
Ubuntu 22.04. The Dockerfile (`docker/spark/Dockerfile`) layers Python 3.11,
`uv`, pre-fetched JARs, and the `skill-radar` project on top.

### Why Java 11 (not 17)

Spark 3.5 officially supports both Java 11 and 17. We pin Java 11 because:

1. **JDK 17 aarch64 stability bug** — Temurin 17.0.18 on `linux/aarch64`
   triggers a deterministic JVM crash (`Internal Error (symbol.cpp:335), fatal
   error: refcount underflow`) during Iceberg read-back operations. The crash
   is a JDK runtime bug that cannot be mitigated with memory tuning or JVM
   flags.
2. **Broad compatibility** — Java 11 is the long-standing baseline for Spark
   3.x and all Iceberg/Hadoop connector JARs; no JAR compatibility issues
   arise.
3. **Official image availability** — Apache publishes a first-party
   `java11-python3-ubuntu` variant, eliminating the need to manually install a
   JDK inside the container.

### Why no manual JDK installation

Previous iterations of the Dockerfile installed a JDK via `apt` (GPG keys,
`dpkg --add-architecture`, symlinks, `JAVA_HOME` overrides). This was fragile
and made architecture-specific bugs hard to diagnose. The current approach
relies entirely on the JDK bundled by the official Spark image at
`/opt/java/openjdk`, removing ~30 lines of manual setup.

### Whole-stage codegen

Whole-stage codegen is **disabled** (`spark.sql.codegen.wholeStage=false` in
`configs/spark-defaults.conf`). JDK 11 on aarch64 has a known
`NullPointerException` in `StackTraceElement.computeFormat()` when Spark's
dynamically-generated codegen classes appear in stack traces (e.g. during
Iceberg error logging). This leads to `free(): invalid pointer` and a JVM
crash. Disabling codegen avoids the issue entirely; the performance impact is
negligible for SkillRadar data volumes (< 200 K rows per table).

### Cross-architecture platform override

Set `SKILLRADAR_DOCKER_PLATFORM` to force a platform when Airflow's
`DockerOperator` launches Spark containers:

```bash
# .env
SKILLRADAR_DOCKER_PLATFORM=linux/amd64
```

When unset (the default), Docker uses the host's native architecture. This is
useful for CI runners or mixed-arch teams where the Spark image may only be
published for one architecture.

### Runtime diagnostics

A quick health check is available inside any Spark container:

```bash
skill-radar run diagnostics
```

This prints the Spark version, Java version, CPU architecture, and Python
version — useful for verifying the runtime after an image rebuild.

---

## Configuration entrypoints

### docker-compose.yml
Defines:
- `minio` service
- `mc` init service (creates buckets)
- `spark` service
- persistent volumes (MinIO data + Spark/Ivy dependency cache)

### configs/spark-defaults.conf
Spark defaults, loaded by Spark at startup:
- S3A endpoint configuration pointing to `http://minio:9000`
- Iceberg catalog configuration (`sr`)
- Maven dependencies (Iceberg runtime + Hadoop AWS)

---

## Developer workflow

### Start the stack
```bash
docker compose up -d
```

### Run smoke test
```bash
docker compose exec spark bash -lc "spark-submit /opt/skillradar/jobs/examples/iceberg_smoke_test.py"
```

### View MinIO console
Open `http://localhost:9001` in your browser and log in with the credentials from `.env`:
- Access Key: `skillradar`
- Secret Key: `skillradar-secret`
You should see the `skillradar-lake` bucket created by the `mc` init service.
