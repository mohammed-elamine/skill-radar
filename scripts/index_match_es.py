from __future__ import annotations

import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ES_URL = "http://127.0.0.1:9200"
INDEX = "job_skill_matches"


def main() -> None:
    spark = SparkSession.builder.master("local[*]").appName("index-matches-to-es").getOrCreate()

    df = spark.read.parquet("data/curated/text/job_skill_matches")

    # S'assurer que dt est bien une date ISO
    df = df.withColumn("dt", F.to_date("dt"))

    # Convertir en RDD JSON pour ES
    docs = df.select(
        "job_id", "dt", "skill_id", "skill_label", "match_method", "match_string", "score"
    ).toJSON()

    # Ecriture vers Elasticsearch via HTTP (foreachPartition)
    import requests

    def send_partition(it):
        s = requests.Session()
        batch = []
        for row_json in it:
            doc = json.loads(row_json)
            # Bulk format: action line + document line
            batch.append(json.dumps({"index": {"_index": INDEX}}))
            batch.append(json.dumps(doc, default=str))

            if len(batch) >= 2000:  # ~1000 docs
                payload = "\n".join(batch) + "\n"
                r = s.post(
                    f"{ES_URL}/_bulk",
                    data=payload,
                    headers={"Content-Type": "application/x-ndjson"},
                )
                r.raise_for_status()
                batch.clear()

        if batch:
            payload = "\n".join(batch) + "\n"
            r = s.post(
                f"{ES_URL}/_bulk", data=payload, headers={"Content-Type": "application/x-ndjson"}
            )
            r.raise_for_status()

    docs.foreachPartition(send_partition)

    spark.stop()


if __name__ == "__main__":
    main()
