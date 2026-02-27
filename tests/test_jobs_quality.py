from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

DB_PATH = Path("data/dev.duckdb")
PARQUET_PATH = Path("data/formatted/jobs_clean.parquet")


def _connect() -> duckdb.DuckDBPyConnection:
    assert DB_PATH.exists(), f"Missing DuckDB database: {DB_PATH}"
    return duckdb.connect(str(DB_PATH))


def test_jobs_clean_table_exists() -> None:
    con = _connect()
    try:
        tables = {t[0] for t in con.execute("show tables").fetchall()}
        assert "jobs_clean" in tables, f"jobs_clean not found. Tables: {sorted(tables)}"
    finally:
        con.close()


def test_jobs_clean_job_id_not_null() -> None:
    con = _connect()
    try:
        n = con.execute("select count(*) from jobs_clean where job_id is null").fetchone()[0]
        assert n == 0, f"Found {n} rows with NULL job_id"
    finally:
        con.close()


def test_jobs_clean_job_id_unique() -> None:
    con = _connect()
    try:
        n = con.execute(
            """
            select count(*) from (
              select job_id
              from jobs_clean
              group by job_id
              having count(*) > 1
            )
            """
        ).fetchone()[0]
        assert n == 0, f"Found {n} duplicated job_id values"
    finally:
        con.close()


def test_jobs_clean_basic_fields_not_blank() -> None:
    """Soft normalization: no empty strings for key fields."""
    con = _connect()
    try:
        n = con.execute(
            """
            select count(*) from jobs_clean
            where trim(coalesce(title, '')) = ''
               or trim(coalesce(company_name, '')) = ''
               or trim(coalesce(location_name, '')) = ''
            """
        ).fetchone()[0]
        assert n == 0, f"Found {n} rows with blank title/company_name/location_name"
    finally:
        con.close()


def test_exported_parquet_exists_and_not_empty() -> None:
    assert PARQUET_PATH.exists(), f"Missing parquet export: {PARQUET_PATH}"
    df = pd.read_parquet(PARQUET_PATH)
    assert len(df) > 0, "Parquet is empty"
    assert "job_id" in df.columns, "job_id missing in parquet"
