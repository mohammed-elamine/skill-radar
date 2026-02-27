from __future__ import annotations

import duckdb

from skill_radar.config import settings


def export_jobs_clean_to_parquet() -> None:
    db_path = settings.duckdb_path
    output_path = settings.data_root / "formatted" / "jobs_clean.parquet"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    con.execute(
        f"""
        COPY jobs_clean
        TO '{output_path.as_posix()}'
        (FORMAT PARQUET);
        """
    )
    con.close()
