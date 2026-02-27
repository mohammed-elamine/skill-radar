from __future__ import annotations

import json

import duckdb

from skill_radar.config import settings


def load_raw_jobs_to_duckdb() -> None:
    raw_jobs_dir = settings.data_root / "raw" / "jobs"
    db_path = settings.duckdb_path

    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.execute("CREATE TABLE IF NOT EXISTS raw_jobs (job_json TEXT)")

    for file in raw_jobs_dir.rglob("jobs_raw_page_*.json"):
        data = json.loads(file.read_text(encoding="utf-8"))
        for job in data.get("results", []):
            con.execute("INSERT INTO raw_jobs VALUES (?)", [json.dumps(job)])

    con.close()
