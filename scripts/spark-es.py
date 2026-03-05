from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ES_NODES = "localhost"
ES_PORT = "9200"
DATA_DIR = "/work/data"

spark = (
    SparkSession.builder.appName("SkillRadar-IndexAll")
    .config("spark.es.nodes", ES_NODES)
    .config("spark.es.port", ES_PORT)
    .config("spark.es.nodes.wan.only", "true")
    .config("spark.es.index.auto.create", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def write_to_es(df, index, id_col=None):
    """Write a DataFrame to an Elasticsearch index via the ES-Spark connector."""
    writer = (
        df.write.format("org.elasticsearch.spark.sql")
        .option("es.resource", index)
        .mode("overwrite")
    )
    if id_col:
        writer = writer.option("es.mapping.id", id_col)
    writer.save()
    print(f"Indexed {df.count()} documents -> {index}")


# ---------------------------------------------------------------------------
# 1. jobs_clean  ->  'jobs'
#    The raw cleaned job postings (150 jobs from Adzuna)
# ---------------------------------------------------------------------------
jobs = spark.read.parquet(f"{DATA_DIR}/formatted/jobs_clean.parquet")
jobs = jobs.withColumn("created_at", F.col("created_at").cast("string"))
write_to_es(jobs, "jobs", id_col="job_id")

# ---------------------------------------------------------------------------
# 2. job_skill_matches  ->  'job_skill_matches'
#    Which ESCO skills were matched in each job description
# ---------------------------------------------------------------------------
matches = spark.read.parquet(f"{DATA_DIR}/curated/text/job_skill_matches")
matches = matches.withColumn("dt", F.col("dt").cast("string"))
write_to_es(matches, "job_skill_matches")

# ---------------------------------------------------------------------------
# 3. skill_kpis_daily  ->  'skill_kpis_daily'
#    Daily demand KPIs per skill (count, share, avg score)
# ---------------------------------------------------------------------------
kpis = spark.read.parquet(f"{DATA_DIR}/curated/kpis/skill_kpis_daily")
kpis = kpis.withColumn("dt", F.col("dt").cast("string"))
write_to_es(kpis, "skill_kpis_daily")

# ---------------------------------------------------------------------------
# 4. top_skills_overall  ->  'top_skills_overall'
#    Aggregate ranking of skills across all collected jobs
# ---------------------------------------------------------------------------
top_overall = spark.read.parquet(f"{DATA_DIR}/curated/analysis/top_skills_overall")
write_to_es(top_overall, "top_skills_overall", id_col="skill_id")

# ---------------------------------------------------------------------------
# 5. top_skills_by_day  ->  'top_skills_by_day'
#    Daily ranking of skills
# ---------------------------------------------------------------------------
top_by_day = spark.read.parquet(f"{DATA_DIR}/curated/analysis/top_skills_by_day")
top_by_day = top_by_day.withColumn("dt", F.col("dt").cast("string"))
write_to_es(top_by_day, "top_skills_by_day")

# ---------------------------------------------------------------------------
# Quick verification -- show row counts read back from ES
# ---------------------------------------------------------------------------
print("\n=== ES index summary ===")
for index in [
    "jobs",
    "job_skill_matches",
    "skill_kpis_daily",
    "top_skills_overall",
    "top_skills_by_day",
]:
    df = spark.read.format("org.elasticsearch.spark.sql").option("es.resource", index).load()
    print(f"  {index}: {df.count()} docs")

spark.stop()
