# Infrastructure (Local, Production-Aligned)

This project uses a **local-first lakehouse stack** that mirrors production patterns:
- **Object storage**: MinIO (S3-compatible)
- **Table format**: Apache Iceberg (ACID tables, schema evolution, snapshots)
- **Compute**: Apache Spark (batch processing)
- (Optional later) **Orchestration**: Airflow
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
- Uses:
  - S3A connector (Hadoop AWS) to read/write to MinIO
  - Iceberg Spark runtime to manage Iceberg tables and namespaces

### Iceberg
- Defines table format and transaction semantics.
- Provides:
  - Atomic commits
  - Snapshot history
  - Schema evolution
  - Partition evolution

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
