import subprocess

from skill_radar.formatting.export_jobs_parquet import export_jobs_clean_to_parquet
from skill_radar.formatting.load_raw_jobs import load_raw_jobs_to_duckdb


def run_dbt() -> None:
    subprocess.run(
        [
            "dbt",
            "run",
            "--project-dir",
            "src/skill_radar/dbt/jobs_dbt",
        ],
        check=True,
    )


def main() -> None:
    load_raw_jobs_to_duckdb()
    run_dbt()
    export_jobs_clean_to_parquet()


if __name__ == "__main__":
    main()
