"""CLI commands for Adzuna dataset operations."""

from __future__ import annotations

import logging
import sys

import click

from skill_radar.platform.logging import finalize_logging, init_logging, set_context

logger = logging.getLogger(__name__)

EXIT_SUCCESS = 0
EXIT_BRONZE_ERROR = 5
EXIT_SILVER_ERROR = 7


@click.group("adzuna")
def adzuna_group() -> None:
    """Adzuna dataset operations (Bronze / Silver)."""


@adzuna_group.command()
@click.option(
    "--preset",
    default=None,
    help="Extraction preset name (default: from config/contract).",
)
@click.option(
    "--country",
    default=None,
    help="Country code override (default: from preset/config).",
)
@click.option(
    "--max-pages",
    "max_pages",
    default=None,
    type=int,
    help="Maximum pages to fetch (default: from config).",
)
@click.option(
    "--results-per-page",
    "results_per_page",
    default=None,
    type=int,
    help="Results per API page (default: from config).",
)
@click.option(
    "--ingestion-date",
    "ingestion_date",
    default=None,
    help="Ingestion date YYYY-MM-DD (default: current UTC date).",
)
@click.option("--quiet", is_flag=True, default=False, help="Suppress normal output.")
def bronze(preset, country, max_pages, results_per_page, ingestion_date, quiet):
    """Run Adzuna Bronze extraction (API → Iceberg)."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo("Error: PySpark is not installed. Bronze requires Spark/Iceberg.")
        sys.exit(EXIT_BRONZE_ERROR)

    from skill_radar.domains.adzuna.bronze.extract import (
        run_bronze_extraction,
        upload_run_summary,
    )

    ctx = init_logging("adzuna_bronze_extract", enable_file=True)
    set_context(dataset="adzuna")

    spark = SparkSession.builder.appName("adzuna_bronze").getOrCreate()
    set_context(spark_app_id=spark.sparkContext.applicationId)

    try:
        result = run_bronze_extraction(
            spark,
            preset=preset,
            country=country,
            max_pages=max_pages,
            results_per_page=results_per_page,
            ingestion_date=ingestion_date,
            run_id=ctx.run_id,
        )

        if result.success:
            if not quiet:
                click.echo("")
                click.echo("  Adzuna Bronze extraction complete.")
                click.echo(f"    Country        : {result.country}")
                click.echo(f"    Preset         : {result.preset}")
                click.echo(f"    Pages fetched  : {result.pages_fetched}")
                click.echo(f"    Rows written   : {result.rows_written}")
                click.echo(f"    Log rows       : {result.request_log_rows}")
                click.echo(f"    Table          : {result.target_table}")
                click.echo(f"    Ingestion date : {result.ingestion_date}")
                click.echo(f"    Run ID         : {result.run_id}")
                click.echo("")
            upload_run_summary(result)
            finalize_logging()
            sys.exit(EXIT_SUCCESS)

        click.echo("")
        click.echo(f"  Bronze extraction failed: {result.error}")
        finalize_logging()
        sys.exit(EXIT_BRONZE_ERROR)

    except Exception as exc:
        logger.exception("Bronze extraction failed")
        click.echo(f"\n  Fatal error: {exc}")
        finalize_logging()
        sys.exit(EXIT_BRONZE_ERROR)
    finally:
        spark.stop()


@adzuna_group.command()
@click.option(
    "--country",
    default=None,
    help="Country code (default: from config).",
)
@click.option(
    "--ingestion-date",
    "ingestion_date",
    default=None,
    help="Ingestion date scope YYYY-MM-DD (default: current UTC date).",
)
@click.option("--quiet", is_flag=True, default=False, help="Suppress normal output.")
def silver(country, ingestion_date, quiet):
    """Run Adzuna Silver formatting (Bronze → Silver Iceberg)."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo("Error: PySpark is not installed. Silver requires Spark/Iceberg.")
        sys.exit(EXIT_SILVER_ERROR)

    from skill_radar.domains.adzuna.silver.format import (
        run_silver_format,
        upload_run_summary,
    )

    ctx = init_logging("adzuna_silver_format", enable_file=True)
    set_context(dataset="adzuna")

    spark = (
        SparkSession.builder.appName("adzuna_silver")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    set_context(spark_app_id=spark.sparkContext.applicationId)

    try:
        result = run_silver_format(
            spark,
            country=country,
            ingestion_date=ingestion_date,
            run_id=ctx.run_id,
        )

        if result.success:
            if not quiet:
                click.echo("")
                click.echo("  Adzuna Silver formatting complete.")
                click.echo(f"    Country          : {result.country}")
                click.echo(f"    Ingestion date   : {result.ingestion_date}")
                click.echo(f"    Input rows       : {result.input_row_count}")
                click.echo(f"    Output rows      : {result.output_row_count}")
                click.echo(f"    Dupes removed    : {result.duplicates_removed}")
                click.echo(f"    Table            : {result.target_table}")
                click.echo(f"    Run ID           : {result.run_id}")
                click.echo("")
            upload_run_summary(result)
            finalize_logging()
            sys.exit(EXIT_SUCCESS)

        click.echo("")
        click.echo(f"  Silver formatting failed: {result.error}")
        finalize_logging()
        sys.exit(EXIT_SILVER_ERROR)

    except Exception as exc:
        logger.exception("Silver formatting failed")
        click.echo(f"\n  Fatal error: {exc}")
        finalize_logging()
        sys.exit(EXIT_SILVER_ERROR)
    finally:
        spark.stop()
