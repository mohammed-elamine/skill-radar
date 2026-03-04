from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from skill_radar.text_pipeline.analysis import top_skills_by_day, top_skills_overall
from skill_radar.text_pipeline.kpis import skill_kpis_daily
from skill_radar.text_pipeline.quality import filter_bad_matches

if TYPE_CHECKING:
    from skill_radar.text_pipeline.contracts import TextPipelinePaths


def build_spark(app_name: str = "skill-radar-text") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


# ----------------------------
# 1) I/O contracts
# ----------------------------
def read_jobs(spark: SparkSession, jobs_path: str) -> DataFrame:
    df = spark.read.parquet(jobs_path)

    required = {"job_id", "title", "description"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Jobs missing required columns: {missing}")

    if "dt" not in df.columns:
        if "created_at" in df.columns:
            df = df.withColumn("dt", F.to_date(F.col("created_at")))
        else:
            df = df.withColumn("dt", F.current_date())

    return df


def read_esco_skills(spark: SparkSession, esco_path: str) -> DataFrame:
    df = spark.read.parquet(esco_path)

    if {"skill_id", "skill_label"}.issubset(df.columns):
        if "aliases" not in df.columns:
            df = df.withColumn("aliases", F.array(F.col("skill_label")))
        return df

    required = {"skilluri", "term", "term_type"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"ESCO missing required columns: {missing}")

    preferred = df.filter(F.col("term_type") == "preferred").select(
        F.col("skilluri").alias("skill_id"), F.col("term").alias("skill_label")
    )

    aliases = (
        df.groupBy("skilluri")
        .agg(F.collect_set("term").alias("aliases"))
        .select(F.col("skilluri").alias("skill_id"), "aliases")
    )

    return preferred.join(aliases, on="skill_id", how="left")


# ----------------------------
# 2) Normalization
# ----------------------------
def normalize_text(df: DataFrame, title_col="title", desc_col="description") -> DataFrame:
    text = F.concat_ws(
        " ", F.coalesce(F.col(title_col), F.lit("")), F.coalesce(F.col(desc_col), F.lit(""))
    )
    text = F.regexp_replace(text, r"<[^>]+>", " ")
    text = F.lower(text)
    text = F.regexp_replace(text, r"[^\p{L}\p{Nd}]+", " ")
    text = F.trim(F.regexp_replace(text, r"\s+", " "))
    return df.withColumn("text_norm", text)


# ----------------------------
# 3) ESCO alias table
# ----------------------------
def build_esco_aliases(esco: DataFrame) -> DataFrame:
    stopwords = [
        "avec",
        "de",
        "des",
        "du",
        "la",
        "le",
        "les",
        "et",
        "en",
        "dans",
        "pour",
        "sur",
        "à",
        "au",
        "aux",
        "un",
        "une",
        "the",
        "a",
        "c",
    ]
    min_chars = 3

    aliases = esco.select(
        "skill_id",
        "skill_label",
        F.explode(F.col("aliases")).alias("alias_raw"),
    )

    alias_norm = F.lower(F.trim(F.col("alias_raw")))
    alias_norm = F.regexp_replace(alias_norm, r"<[^>]+>", " ")
    alias_norm = F.regexp_replace(alias_norm, r"[^\p{L}\p{Nd}]+", " ")
    alias_norm = F.trim(F.regexp_replace(alias_norm, r"\s+", " "))

    aliases = aliases.withColumn("alias_norm", alias_norm)
    aliases = aliases.withColumn("alias_len", F.length("alias_norm"))
    aliases = aliases.withColumn("alias_tokens", F.split(F.col("alias_norm"), " "))
    aliases = aliases.withColumn("alias_token_count", F.size(F.col("alias_tokens")))

    stopwords_arr = F.array(*[F.lit(w) for w in stopwords])

    aliases = (
        aliases.filter(F.length("alias_norm") > 0)
        .filter(F.col("alias_len") >= F.lit(min_chars))
        .filter(~F.array_contains(stopwords_arr, F.col("alias_norm")))
        .filter(~((F.col("alias_token_count") == 1) & (F.col("alias_len") < 4)))
        .dropDuplicates(["skill_id", "alias_norm"])
    )

    return aliases.select("skill_id", "skill_label", "alias_raw", "alias_norm", "alias_len")


# ----------------------------
# 4) Tokenize + n-grams
# ----------------------------
def tokenize(df: DataFrame) -> DataFrame:
    return df.withColumn("tokens", F.split(F.col("text_norm"), " "))


def make_ngrams(df: DataFrame, n_min=1, n_max=5) -> DataFrame:
    ngram_arrays = []
    for n in range(n_min, n_max + 1):
        expr = f"""
        transform(
            sequence(1, greatest(size(tokens) - {n} + 1, 0)),
            i -> array_join(slice(tokens, i, {n}), ' ')
        )
        """
        ngram_arrays.append(F.expr(expr))

    all_ngrams = F.array_distinct(F.flatten(F.array(*ngram_arrays)))
    return df.withColumn("ngrams", all_ngrams)


# ----------------------------
# 5) Exact matching via join
# ----------------------------
def match_skills_exact(jobs_with_ngrams: DataFrame, esco_aliases: DataFrame) -> DataFrame:
    exploded = jobs_with_ngrams.select(
        "job_id",
        "dt",
        F.explode("ngrams").alias("candidate"),
    ).filter(F.length("candidate") > 0)

    aliases = F.broadcast(esco_aliases.select("skill_id", "skill_label", "alias_norm"))

    matches = (
        exploded.join(aliases, exploded["candidate"] == aliases["alias_norm"], "inner")
        .withColumn("match_method", F.lit("ngram_exact"))
        .withColumn("match_string", F.col("candidate"))
        .withColumn("score", F.lit(1.0))
        .select("job_id", "dt", "skill_id", "skill_label", "match_method", "match_string", "score")
        .dropDuplicates(["job_id", "dt", "skill_id"])
    )
    return matches


# ----------------------------
# 7) Main runner
# ----------------------------
def run(paths: TextPipelinePaths, spark: SparkSession | None = None) -> None:
    spark = spark or build_spark()
    p = paths.as_spark()

    # --- Read
    jobs = read_jobs(spark, p.jobs_input)
    esco = read_esco_skills(spark, p.esco_input)

    # --- Transform
    jobs_n = normalize_text(jobs)
    jobs_t = tokenize(jobs_n)
    jobs_g = make_ngrams(jobs_t, n_min=1, n_max=5)

    esco_aliases = build_esco_aliases(esco)

    # --- Match + quality
    matches = match_skills_exact(jobs_g, esco_aliases)
    matches = filter_bad_matches(matches, min_term_len=3)

    # ✅ Write MATCHES output (the core curated dataset)
    matches.write.mode("overwrite").parquet(p.matches_output)

    # ✅ KPIs
    kpis = skill_kpis_daily(matches)
    kpis.write.mode("overwrite").parquet(p.kpis_output)

    # ✅ Analysis outputs (optional but useful)
    analysis_root = "data/curated/analysis"
    top_all = top_skills_overall(matches, top_n=200)
    top_day = top_skills_by_day(matches, top_n=200)

    top_all.write.mode("overwrite").parquet(f"{analysis_root}/top_skills_overall")
    top_day.write.mode("overwrite").parquet(f"{analysis_root}/top_skills_by_day")

    spark.stop()
