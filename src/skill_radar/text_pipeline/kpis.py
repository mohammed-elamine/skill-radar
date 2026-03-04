from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def skill_kpis_daily(matches: DataFrame) -> DataFrame:
    total = (
        matches.select("dt", "job_id")
        .distinct()
        .groupBy("dt")
        .agg(F.count("*").alias("total_jobs"))
    )

    kpi = (
        matches.select("dt", "job_id", "skill_id", "skill_label", "score")
        .groupBy("dt", "skill_id", "skill_label")
        .agg(
            F.countDistinct("job_id").alias("count_jobs"),
            F.count("*").alias("count_matches"),
            F.avg("score").alias("avg_score"),
        )
        .join(total, on="dt", how="left")
        .withColumn("share", F.col("count_jobs") / F.col("total_jobs"))
        .orderBy("dt", F.desc("count_jobs"))
    )
    return kpi
