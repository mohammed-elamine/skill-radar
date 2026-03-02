from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from skill_radar.utils.assertions import require
from skill_radar.utils.logging import setup_logging


def main() -> None:
    log = setup_logging("iceberg_smoke_test")

    spark = SparkSession.builder.appName("iceberg_smoke_test").getOrCreate()
    log.info("Spark started")

    # Create namespaces aligned with future pipeline layers
    for ns in ["bronze", "silver", "gold"]:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS sr.{ns}")
        log.info("Ensured namespace sr.%s", ns)

    # Write into a realistic namespace
    table = "sr.silver.iceberg_smoke"

    df = (
        spark.range(1, 6)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("tag", F.lit("smoke"))
    )

    df.writeTo(table).using("iceberg").createOrReplace()
    log.info("Wrote Iceberg table: %s", table)

    out = spark.table(table).orderBy("id")
    log.info("Read back from Iceberg table: %s", table)
    out.show(truncate=False)

    count = out.count()
    require(count == 5, f"Expected 5 rows, got {count}")
    log.info("Row count check OK: %d", count)

    # Small metadata sanity check
    cols = set(out.columns)
    require(
        "id" in cols and "ingested_at" in cols and "tag" in cols,
        f"Unexpected columns: {out.columns}",
    )
    log.info("Schema check OK")

    spark.stop()
    log.info("✅ Iceberg smoke test passed.")


if __name__ == "__main__":
    main()
