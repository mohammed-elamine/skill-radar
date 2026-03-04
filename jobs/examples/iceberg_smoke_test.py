import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from skill_radar.platform.logging import init_logging
from skill_radar.utils.assertions import require


def main() -> None:
    init_logging(job_name="iceberg_smoke_test", verbose=False)
    log = logging.getLogger(__name__)

    spark = SparkSession.builder.appName("iceberg_smoke_test").getOrCreate()
    log.info("Spark started")

    for ns in ["bronze", "silver", "gold"]:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS sr.{ns}")
        log.info("Ensured namespace sr.%s", ns)

    table = "sr.silver.iceberg_smoke"
    df = (
        spark.range(1, 6)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("tag", F.lit("smoke"))
    )

    df.writeTo(table).using("iceberg").createOrReplace()
    log.info("Wrote Iceberg table: %s", table)

    out = spark.table(table).orderBy("id")
    out.show(truncate=False)

    count = out.count()
    require(count == 5, f"Expected 5 rows, got {count}")
    log.info("Row count check OK: %d", count)

    cols = set(out.columns)
    require({"id", "ingested_at", "tag"}.issubset(cols), f"Unexpected columns: {out.columns}")
    log.info("Schema check OK")

    spark.stop()
    log.info("✅ Iceberg smoke test passed.")


if __name__ == "__main__":
    main()
