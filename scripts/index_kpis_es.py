from __future__ import annotations

import json

import requests
from pyspark.sql import SparkSession

ES_URL = "http://127.0.0.1:9200"
INDEX = "skill_kpis_daily"


def send_bulk(docs):
    payload = "\n".join(docs) + "\n"
    r = requests.post(
        f"{ES_URL}/_bulk",
        data=payload,
        headers={"Content-Type": "application/x-ndjson"},
    )
    r.raise_for_status()


def main():
    spark = SparkSession.builder.master("local[*]").appName("index-kpis").getOrCreate()

    df = spark.read.parquet("data/curated/kpis/skill_kpis_daily")

    docs = []
    for row in df.toJSON().collect():
        docs.append(json.dumps({"index": {"_index": INDEX}}))
        docs.append(row)

    send_bulk(docs)

    print("Indexed", len(docs) // 2, "documents")

    spark.stop()


if __name__ == "__main__":
    main()
