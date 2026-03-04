from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = SparkSession.builder.master("local[*]").appName("inspect-matches").getOrCreate()

    matches = spark.read.parquet("data/curated/text/job_skill_matches")
    kpis = spark.read.parquet("data/curated/kpis/skill_kpis_daily")

    # ✅ Lire jobs_clean pour récupérer title/company/location
    jobs = (
        spark.read.parquet("data/formatted/jobs_clean.parquet")
        .select("job_id", "title", "company_name", "location_name")
        .dropDuplicates(["job_id"])
    )

    print("\n=== COUNTS ===")
    print("matches rows:", matches.count())
    print("unique jobs in matches:", matches.select("job_id").distinct().count())
    print("unique skills matched:", matches.select("skill_id").distinct().count())

    print("\n=== TOP SKILLS (by count_jobs) ===")
    kpis.orderBy(F.desc("count_jobs")).select("skill_label", "count_jobs", "share").show(
        30, truncate=False
    )

    print("\n=== MOST COMMON match_string (spot false positives) ===")
    matches.groupBy("match_string").count().orderBy(F.desc("count")).show(30, truncate=False)

    print("\n=== SAMPLE MATCHES (with title/company/location) ===")
    sample = (
        matches.join(jobs, on="job_id", how="left")
        .select(
            "job_id",
            "title",
            "company_name",
            "location_name",
            "skill_label",
            "match_string",
            "score",
        )
        .orderBy("job_id", "skill_label")
    )
    sample.show(30, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
