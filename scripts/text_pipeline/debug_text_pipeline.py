import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from skill_radar.config import Settings
from skill_radar.text_pipeline.run import (
    build_esco_aliases,
    make_ngrams,
    match_skills_exact,
    normalize_text,
    read_esco_skills,
    read_jobs,
    tokenize,
)


def p(pth: str | Path) -> str:
    return str(Path(pth).expanduser())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default=None)
    parser.add_argument("--limit", type=int, default=20)
    args = parser.parse_args()

    settings = Settings.from_env()
    if args.data_root:
        settings = settings.with_data_root(args.data_root)

    jobs_path = settings.formatted_jobs_partitioned_root
    esco_path = settings.esco_root

    spark = SparkSession.builder.master("local[*]").appName("debug-text").getOrCreate()

    print(">> jobs_path =", jobs_path)
    print(">> esco_path =", esco_path)

    jobs = read_jobs(spark, p(jobs_path))
    esco = read_esco_skills(spark, p(esco_path))

    print("\n>> == INPUT COUNTS ==")
    print(">> jobs:", jobs.count())
    print(">> esco:", esco.count())

    print("\n>> == JOBS SAMPLE ==")
    jobs.select("job_id", "dt", "title", "description").show(args.limit, truncate=False)

    print("\n>> == ESCO SAMPLE ==")
    esco.select("skill_id", "skill_label", "aliases").show(args.limit, truncate=False)

    # ---- ESCO aliases normalization ----
    esco_aliases = build_esco_aliases(esco)

    print("\n>> == ESCO ALIASES (normalized) SAMPLE ==")
    esco_aliases.select("skill_id", "skill_label", "alias_raw", "alias_norm", "alias_len").orderBy(
        F.desc("alias_len")
    ).show(args.limit, truncate=False)

    print("\n>> == ESCO aliases count ==")
    print(">>", esco_aliases.count())

    # ---- Jobs text pipeline ----
    jobs_n = normalize_text(jobs)
    print("\n>> == JOBS NORMALIZED TEXT SAMPLE ==")
    jobs_n.select("job_id", "dt", "text_norm").show(5, truncate=False)

    jobs_t = tokenize(jobs_n)
    print("\n>> == JOBS TOKENIZED SAMPLE ==")
    jobs_t.select("job_id", "dt", "text_norm", "tokens").show(5, truncate=False)

    jobs_g = make_ngrams(jobs_t, n_min=1, n_max=5)
    print("\n>> == JOB TOKENS/ NGRAMS SAMPLE ==")
    jobs_g.select("job_id", "dt", "text_norm", "tokens", "ngrams").show(5, truncate=False)

    # ---- Matching ----
    matches = match_skills_exact(jobs_g, esco_aliases)

    print("\n>> == MATCHES COUNT ==")
    print(">>", matches.count())

    print("\n>> == MATCHES SAMPLE ==")
    matches.select(
        "job_id", "dt", "skill_id", "skill_label", "match_string", "match_method", "score"
    ).orderBy("job_id", "skill_label").show(args.limit, truncate=False)

    # Helpful: see which step drops everything
    print("\n>> == QUICK DIAGNOSTIC ==")
    print(">> jobs with zero ngrams:", jobs_g.filter(F.size("ngrams") == 0).count())
    print(">> aliases empty:", esco_aliases.filter(F.length("alias_norm") == 0).count())

    spark.stop()


if __name__ == "__main__":
    main()
