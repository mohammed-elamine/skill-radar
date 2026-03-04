from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("inspect-real-data").getOrCreate()

base = Path("data/formatted")
paths = [
    base / "jobs_clean.parquet",
    base / "esco_skill_terms.parquet",  # si tu l'as
    base / "esco_occ_skill_rel.parquet",
    base / "esco_skills.parquet",
]

for p in paths:
    if not p.exists():
        print(f"\n❌ Missing: {p}")
        continue

    print(f"\n================= {p.name} =================")
    df = spark.read.parquet(str(p))
    df.printSchema()
    print("rows:", df.count())
    df.show(5, truncate=80)

spark.stop()
