from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def top_skills_overall(matches: DataFrame, top_n: int = 200) -> DataFrame:
    """
    Output columns:
      - skill_id, skill_label
      - count_matches (rows)
      - count_jobs (distinct job_id)
      - share_jobs (count_jobs / total_jobs_in_matches)
      - avg_score
    """
    total_jobs = matches.select("job_id").distinct().count()

    out = (
        matches.groupBy("skill_id", "skill_label")
        .agg(
            F.count("*").alias("count_matches"),
            F.countDistinct("job_id").alias("count_jobs"),
            F.avg("score").alias("avg_score"),
        )
        .withColumn("total_jobs", F.lit(int(total_jobs)))
        .withColumn("share_jobs", F.col("count_jobs") / F.col("total_jobs"))
        .orderBy(F.desc("count_jobs"), F.desc("avg_score"))
        .limit(int(top_n))
    )
    return out


def top_skills_by_day(matches: DataFrame, top_n: int = 200) -> DataFrame:
    """
    Output columns:
      - dt
      - skill_id, skill_label
      - count_matches
      - count_jobs
      - total_jobs_dt
      - share_jobs_dt
      - avg_score
      - rank_dt (by count_jobs then avg_score)
    """
    # total distinct jobs per day
    total_jobs_dt = (
        matches.select("dt", "job_id")
        .distinct()
        .groupBy("dt")
        .agg(F.count("*").alias("total_jobs_dt"))
    )

    agg = (
        matches.groupBy("dt", "skill_id", "skill_label")
        .agg(
            F.count("*").alias("count_matches"),
            F.countDistinct("job_id").alias("count_jobs"),
            F.avg("score").alias("avg_score"),
        )
        .join(total_jobs_dt, on="dt", how="left")
        .withColumn("share_jobs_dt", F.col("count_jobs") / F.col("total_jobs_dt"))
    )

    w = Window.partitionBy("dt").orderBy(F.desc("count_jobs"), F.desc("avg_score"))
    ranked = agg.withColumn("rank_dt", F.row_number().over(w)).filter(
        F.col("rank_dt") <= int(top_n)
    )

    return ranked.orderBy("dt", "rank_dt")
