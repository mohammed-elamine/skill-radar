from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from skill_radar.config import Settings
from skill_radar.text_pipeline.__main__ import build_paths


def assert_true(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def main(matches_path: str, kpis_path: str, jobs_path: str | None = None) -> None:
    spark = SparkSession.builder.master("local[*]").appName("skill-radar-text-qa").getOrCreate()

    print("---- QA: Skill Radar Text Pipeline ----")
    print(f"Reading matches from: {matches_path}")
    print(f"Reading kpis from: {kpis_path}")
    if jobs_path:
        print(f"Reading jobs from: {jobs_path}")
    print("----")

    matches = spark.read.parquet(matches_path)
    kpis = spark.read.parquet(kpis_path)

    # ---- Schema checks (adjust if you add columns) ----
    required_matches = {
        "job_id",
        "dt",
        "skill_id",
        "skill_label",
        "match_method",
        "match_string",
        "score",
    }
    assert_true(
        required_matches.issubset(set(matches.columns)),
        f"matches missing columns: {required_matches - set(matches.columns)}",
    )

    required_kpis = {"dt", "skill_id", "skill_label", "count_jobs", "total_jobs", "share"}
    assert_true(
        required_kpis.issubset(set(kpis.columns)),
        f"kpis missing columns: {required_kpis - set(kpis.columns)}",
    )

    # ---- Basic volume checks ----
    m_cnt = matches.count()
    k_cnt = kpis.count()
    assert_true(m_cnt > 0, "matches is empty")
    assert_true(k_cnt > 0, "kpis is empty")

    # ---- Uniqueness: one row per (job_id, dt, skill_id) ----
    dup = matches.groupBy("job_id", "dt", "skill_id").count().filter(F.col("count") > 1).count()
    assert_true(dup == 0, f"Found duplicate (job_id,dt,skill_id) in matches: {dup}")

    # ---- Null / empty checks ----
    nulls = (
        matches.select(
            F.sum(F.col("job_id").isNull().cast("int")).alias("job_id_nulls"),
            F.sum(F.col("skill_id").isNull().cast("int")).alias("skill_id_nulls"),
            F.sum((F.length("skill_label") == 0).cast("int")).alias("skill_label_empty"),
        )
        .collect()[0]
        .asDict()
    )
    assert_true(nulls["job_id_nulls"] == 0, f"matches has null job_id: {nulls}")
    assert_true(nulls["skill_id_nulls"] == 0, f"matches has null skill_id: {nulls}")
    assert_true(nulls["skill_label_empty"] == 0, f"matches has empty skill_label: {nulls}")

    # ---- KPI integrity ----
    bad_share = kpis.filter((F.col("share") < 0) | (F.col("share") > 1)).count()
    assert_true(bad_share == 0, f"kpis has share outside [0,1]: {bad_share}")

    bad_counts = kpis.filter((F.col("count_jobs") < 0) | (F.col("total_jobs") <= 0)).count()
    assert_true(bad_counts == 0, f"kpis has invalid counts: {bad_counts}")

    # ---- Optional: consistency with jobs totals ----
    if jobs_path:
        jobs = spark.read.parquet(jobs_path)
        jobs_totals = (
            jobs.select("job_id", "dt")
            .dropDuplicates()
            .groupBy("dt")
            .agg(F.countDistinct("job_id").alias("jobs_total_from_jobs"))
        )
        kpis_totals = kpis.select("dt", "total_jobs").dropDuplicates(["dt", "total_jobs"])

        joined = jobs_totals.join(kpis_totals, on="dt", how="inner")
        mism = joined.filter(F.col("jobs_total_from_jobs") != F.col("total_jobs")).count()
        assert_true(mism == 0, f"Mismatch between jobs totals and kpis totals (by dt): {mism}")

    # ---- Human-friendly summary ----
    print("\n✅ QA PASSED")
    print(f"matches rows: {m_cnt}")
    print(f"kpis rows: {k_cnt}")

    print("\nTop skills by count_jobs:")
    kpis.orderBy(F.desc("count_jobs")).show(15, truncate=False)

    print("\nMost common match_strings (helps spot false positives like 'go'):")
    matches.groupBy("match_string").count().orderBy(F.desc("count")).show(20, truncate=False)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="QA script for skill radar text pipeline outputs. Checks schema, volumes, uniqueness, KPI integrity."
    )

    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        "--data-root",
        type=str,
        default=None,
        help="Root of data lake. Paths are derived by convention.",
    )

    group.add_argument(
        "--paths",
        action="store_true",
        help="Use explicit input/output paths (provide the --jobs-input/--esco-input/... flags).",
    )

    # Explicit path flags (only meaningful when --paths is used)
    parser.add_argument("--matches", type=str, default=None)
    parser.add_argument("--kpis", type=str, default=None)
    parser.add_argument("--jobs", type=str, default=None)

    args = parser.parse_args()

    settings = Settings.from_env()
    if args.data_root:
        settings = settings.with_data_root(args.data_root)
        paths = build_paths(settings=settings)
    else:
        paths = build_paths(
            settings=settings,
            jobs_input=args.jobs_input,
            esco_input=args.esco_input,
            matches_output=args.matches_output,
            kpis_output=args.kpis_output,
        )

    if not paths.matches_output.exists():
        raise FileNotFoundError(f"Matches root does not exist: {paths.matches_output}")

    if not any(paths.matches_output.rglob("part-*")):
        raise RuntimeError(f"No parquet part files found under {paths.matches_output}")

    p = paths.as_spark()

    main(matches_path=p.matches_output, kpis_path=p.kpis_output, jobs_path=p.jobs_input)
