from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from skill_radar.text_pipeline.contracts import TextPipelinePaths


def build_spark(app_name: str = "skill-radar-text") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # tune later; keep defaults minimal for now
        .getOrCreate()
    )


# ----------------------------
# 1) I/O contracts
# ----------------------------
def read_jobs(spark: SparkSession, jobs_path: str) -> DataFrame:
    df = spark.read.parquet(jobs_path)
    required = {"job_id", "title", "description"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Jobs missing required columns: {missing}")

    # dt is optional; derive if absent (you can adapt)
    if "dt" not in df.columns:
        df = df.withColumn("dt", F.current_date())

    return df


def read_esco_skills(spark: SparkSession, esco_path: str) -> DataFrame:
    df = spark.read.parquet(esco_path)
    required = {"skill_id", "skill_label"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"ESCO missing required columns: {missing}")

    # Expect either aliases array, or just use skill_label as alias
    if "aliases" not in df.columns:
        df = df.withColumn("aliases", F.array(F.col("skill_label")))

    return df


# ----------------------------
# 2) Normalization
# ----------------------------
def normalize_text(df: DataFrame, title_col="title", desc_col="description") -> DataFrame:
    # Combine + basic HTML stripping + punctuation normalization
    text = F.concat_ws(
        " ", F.coalesce(F.col(title_col), F.lit("")), F.coalesce(F.col(desc_col), F.lit(""))
    )

    # Remove HTML tags (simple heuristic)
    text = F.regexp_replace(text, r"<[^>]+>", " ")
    # Lowercase
    text = F.lower(text)
    # Replace non-letter/digit with space (keeps accents; refine if needed)
    text = F.regexp_replace(text, r"[^\p{L}\p{Nd}]+", " ")
    # Collapse whitespace
    text = F.trim(F.regexp_replace(text, r"\s+", " "))

    return df.withColumn("text_norm", text)


# ----------------------------
# 3) ESCO alias table
# ----------------------------
def build_esco_aliases(esco: DataFrame) -> DataFrame:
    aliases = esco.select(
        "skill_id",
        "skill_label",
        F.explode(F.col("aliases")).alias("alias_raw"),
    )

    alias_norm = F.lower(F.trim(F.col("alias_raw")))
    alias_norm = F.regexp_replace(alias_norm, r"<[^>]+>", " ")
    alias_norm = F.regexp_replace(alias_norm, r"[^\p{L}\p{Nd}]+", " ")
    alias_norm = F.trim(F.regexp_replace(alias_norm, r"\s+", " "))

    aliases = (
        aliases.withColumn("alias_norm", alias_norm)
        .filter(F.length("alias_norm") > 0)
        .dropDuplicates(["skill_id", "alias_norm"])
        .withColumn("alias_len", F.length("alias_norm"))
    )

    return aliases


# ----------------------------
# 4) Tokenize + n-grams
# ----------------------------
def tokenize(df: DataFrame) -> DataFrame:
    # Split by space into tokens
    return df.withColumn("tokens", F.split(F.col("text_norm"), " "))


def make_ngrams(df: DataFrame, n_min=1, n_max=5) -> DataFrame:
    """
    Create a candidate set of ngrams as strings. Spark doesn't have built-in ngram
    generator via SQL functions; we implement via higher-order functions.
    """
    tokens_col = "tokens"
    ngram_arrays = []

    for n in range(n_min, n_max + 1):
        # sequence(1, greatest(size(tokens)-n+1, 0))
        # transform(starts, i -> array_join(slice(tokens, i, n), ' '))
        expr = f"""
        transform(
            sequence(1, greatest(size({tokens_col}) - {n} + 1, 0)),
            i -> array_join(slice({tokens_col}, i, {n}), ' ')
        )
        """
        ngram_arrays.append(F.expr(expr))

    all_ngrams = F.array_distinct(F.flatten(F.array(*ngram_arrays)))
    return df.withColumn("ngrams", all_ngrams)


# ----------------------------
# 5) Exact matching via join
# ----------------------------
def match_skills_exact(jobs_with_ngrams: DataFrame, esco_aliases: DataFrame) -> DataFrame:
    # Explode ngrams for join
    exploded = jobs_with_ngrams.select(
        "job_id",
        "dt",
        "text_norm",
        *[
            c
            for c in jobs_with_ngrams.columns
            if c not in {"ngrams"} and c in {"country", "city", "role_family", "source"}
        ],
        F.explode("ngrams").alias("candidate"),
    ).filter(F.length("candidate") > 0)

    # Broadcast aliases (often fits in memory; if ESCO huge, remove broadcast)
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
# 6) KPIs
# ----------------------------
def compute_skill_kpis(matches: DataFrame, jobs: DataFrame, dims=("dt",)) -> DataFrame:
    dims_list = list(dims)
    # total jobs per bucket
    keys = ["job_id", *dims_list]
    base = jobs.select("job_id", *dims).dropDuplicates(keys)
    totals = base.groupBy(*dims).agg(F.countDistinct("job_id").alias("total_jobs"))

    counts = matches.groupBy(*dims, "skill_id", "skill_label").agg(
        F.countDistinct("job_id").alias("count_jobs")
    )

    kpis = counts.join(totals, on=dims_list, how="left").withColumn(
        "share", F.col("count_jobs") / F.col("total_jobs")
    )

    # 7-day moving average on share (requires dt as date)
    # If dt is string, cast it: F.to_date("dt")
    if "dt" in dims:
        kpis = kpis.withColumn("dt_date", F.to_date("dt"))
        w = Window.partitionBy("skill_id").orderBy(F.col("dt_date")).rowsBetween(-6, 0)
        kpis = kpis.withColumn("share_ma7", F.avg("share").over(w)).drop("dt_date")

    return kpis


# ----------------------------
# 7) Main runner
# ----------------------------
def run(paths: TextPipelinePaths, spark: SparkSession | None = None) -> None:
    spark = spark or build_spark()

    p = paths.as_spark()

    jobs = read_jobs(spark, p.jobs_input)
    esco = read_esco_skills(spark, p.esco_input)

    jobs_n = normalize_text(jobs)
    jobs_t = tokenize(jobs_n)
    jobs_g = make_ngrams(jobs_t, n_min=1, n_max=5)

    esco_aliases = build_esco_aliases(esco)

    matches = match_skills_exact(jobs_g, esco_aliases)

    # choose dims you care about (dt mandatory for trends)
    dims = ("dt",)  # add "country", "role_family", etc if available
    kpis = compute_skill_kpis(matches, jobs, dims=dims)

    matches.write.mode("overwrite").parquet(p.matches_output)
    kpis.write.mode("overwrite").parquet(p.kpis_output)
