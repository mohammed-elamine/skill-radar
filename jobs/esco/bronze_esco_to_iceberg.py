"""ESCO Bronze extraction — Spark entrypoint.

Run via spark-submit::

    docker compose exec -T spark bash -lc \\
        "spark-submit /opt/skillradar/jobs/esco/bronze_esco_to_iceberg.py \\
            --version v1.2.0 --lang fr"

Arguments
---------
--version   (required)  ESCO artifact version.
--lang      (required)  Language code.
--entities  (optional)  Comma-separated subset (e.g. skills,relations).
--fail-fast (flag)      Abort on first entity error (default: true).
--dry-run   (flag)      Validate plan without writing to Iceberg.
"""

from __future__ import annotations

import argparse
import json
import sys

from pyspark.sql import SparkSession

from skill_radar.domains.esco.bronze.extract import (
    run_bronze_extraction,
    upload_run_summary,
)
from skill_radar.platform.logging import finalize_logging, init_logging, set_context


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ESCO Bronze extraction: CSV → Iceberg",
    )
    p.add_argument("--version", required=True, help="Artifact version (e.g. v1.2.0)")
    p.add_argument("--lang", required=True, help="Language code (e.g. fr)")
    p.add_argument(
        "--entities",
        default=None,
        help="Comma-separated entity names to process (default: all)",
    )
    p.add_argument(
        "--fail-fast",
        action="store_true",
        default=True,
        help="Abort on first entity error (default: true)",
    )
    p.add_argument(
        "--no-fail-fast",
        action="store_false",
        dest="fail_fast",
        help="Continue processing remaining entities on error",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Validate and show plan without writing to Iceberg",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)

    # ── Logging ──────────────────────────────────────────────────────────
    ctx = init_logging("esco_bronze_extract", enable_file=not args.dry_run)
    set_context(dataset="esco", version=args.version, lang=args.lang)

    # ── Spark session ────────────────────────────────────────────────────
    spark = SparkSession.builder.appName(f"esco_bronze_{args.version}_{args.lang}").getOrCreate()

    # Store Spark app id in logging context
    set_context(spark_app_id=spark.sparkContext.applicationId)

    entities = args.entities.split(",") if args.entities else None

    try:
        result = run_bronze_extraction(
            spark,
            args.version,
            args.lang,
            entities=entities,
            fail_fast=args.fail_fast,
            dry_run=args.dry_run,
            run_id=ctx.run_id,
        )

        # Print summary
        print(json.dumps(result.summary_dict(), indent=2, default=str))

        # Upload run summary
        if not args.dry_run:
            upload_run_summary(result)

        if not result.success:
            sys.exit(1)

    except Exception:
        import logging

        logging.getLogger(__name__).exception("Bronze extraction failed")
        sys.exit(2)
    finally:
        finalize_logging(upload=not args.dry_run)
        spark.stop()


if __name__ == "__main__":
    main()
