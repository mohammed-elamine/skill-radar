"""CLI commands for search serving operations.

Provides thin entrypoints for:
- skill-radar search export           (Gold → Elasticsearch)
- skill-radar search bootstrap-kibana (create data views / saved objects)
"""

from __future__ import annotations

import logging
import sys

import click

from skill_radar.domains.search.datasets import ALL_DATASET_NAMES, PRIMARY_DATASETS
from skill_radar.platform.logging import finalize_logging, init_logging, set_context

logger = logging.getLogger(__name__)

EXIT_SUCCESS = 0
EXIT_SEARCH_ERROR = 11


@click.group("search")
def search_group() -> None:
    """Search serving operations (Gold → Elasticsearch)."""


# ---------------------------------------------------------------------------
# search export
# ---------------------------------------------------------------------------


@search_group.command("export")
@click.option(
    "--dataset",
    "dataset",
    default=None,
    help=(
        "Dataset to export (one of: "
        + ", ".join(ALL_DATASET_NAMES)
        + "). Omit for primary datasets; use 'all' for everything."
    ),
)
@click.option(
    "--ingestion-date", "ingestion_date", required=True, help="Partition date YYYY-MM-DD."
)
@click.option("--country", required=True, help="Country code (e.g. fr).")
@click.option("--es-url", "es_url", default=None, help="Elasticsearch URL override.")
@click.option("--create-index/--no-create-index", default=True, help="Create index if missing.")
@click.option("--refresh/--no-refresh", default=True, help="Refresh index after load.")
@click.option("--alias-swap/--no-alias-swap", "alias_swap", default=True, help="Update alias.")
@click.option("--dry-run", is_flag=True, default=False, help="Transform only, no indexing.")
@click.option("--quiet", is_flag=True, default=False, help="Suppress normal output.")
def export_cmd(
    dataset: str | None,
    ingestion_date: str,
    country: str,
    es_url: str | None,
    create_index: bool,
    refresh: bool,
    alias_swap: bool,
    dry_run: bool,
    quiet: bool,
) -> None:
    """Export Gold tables to Elasticsearch.

    Reads selected Gold Iceberg partitions and bulk-indexes them into
    Elasticsearch with deterministic document IDs for idempotent reruns.

    \b
    Examples:
        skill-radar search export --ingestion-date 2026-03-06 --country fr
        skill-radar search export --dataset skill_demand_daily --ingestion-date 2026-03-06 --country fr
        skill-radar search export --dataset all --ingestion-date 2026-03-06 --country fr --dry-run
    """
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo("Error: PySpark is not installed. Search export requires Spark/Iceberg.")
        sys.exit(EXIT_SEARCH_ERROR)

    from skill_radar.config.loader import load_platform_config
    from skill_radar.domains.search.export import run_search_export

    ctx = init_logging("search_export", enable_file=True)
    set_context(dataset="search")

    config = load_platform_config()

    # Resolve datasets
    if dataset is None:
        target_datasets = PRIMARY_DATASETS
    elif dataset == "all":
        target_datasets = ALL_DATASET_NAMES
    elif dataset in ALL_DATASET_NAMES:
        target_datasets = [dataset]
    else:
        click.echo(f"Error: Unknown dataset '{dataset}'. Available: {ALL_DATASET_NAMES}")
        sys.exit(EXIT_SEARCH_ERROR)

    spark = (
        SparkSession.builder.appName("search_export")
        .config("spark.sql.codegen.wholeStage", "false")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    set_context(spark_app_id=spark.sparkContext.applicationId)

    try:
        result = run_search_export(
            spark,
            ingestion_date=ingestion_date,
            country=country,
            datasets=target_datasets,
            config=config,
            es_url=es_url,
            create_index=create_index,
            refresh=refresh,
            alias_swap=alias_swap,
            dry_run=dry_run,
            run_id=ctx.run_id,
        )

        if result.success:
            if not quiet:
                click.echo("")
                click.echo("  Search export complete.")
                click.echo(f"    Country              : {country}")
                click.echo(f"    Ingestion date       : {ingestion_date}")
                click.echo(f"    Datasets exported    : {result.datasets_exported}")
                click.echo(f"    Dry run              : {dry_run}")
                for ds_name, ds_result in result.results.items():
                    click.echo(
                        f"    {ds_name:30s}: "
                        f"{ds_result.success_count} docs → {ds_result.index_name}"
                    )
                click.echo(f"    Run ID               : {result.run_id}")
                click.echo("")
            finalize_logging()
            sys.exit(EXIT_SUCCESS)

        click.echo("")
        click.echo(f"  Search export failed: {result.error}")
        for ds_name, ds_result in result.results.items():
            if ds_result.errors:
                click.echo(f"    {ds_name}: {ds_result.errors[:3]}")
        finalize_logging()
        sys.exit(EXIT_SEARCH_ERROR)

    except Exception as exc:
        logger.exception("Search export failed")
        click.echo(f"\n  Fatal error: {exc}")
        finalize_logging()
        sys.exit(EXIT_SEARCH_ERROR)
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# search bootstrap-kibana
# ---------------------------------------------------------------------------


@search_group.command("bootstrap-kibana")
@click.option(
    "--apply/--write-artifacts-only",
    "apply_api",
    default=False,
    help="Apply data views via Kibana API (default: write NDJSON artifacts only).",
)
@click.option("--force", is_flag=True, default=False, help="Overwrite existing data views.")
@click.option("--kibana-url", "kibana_url", default=None, help="Kibana URL override.")
@click.option("--quiet", is_flag=True, default=False, help="Suppress normal output.")
def bootstrap_kibana_cmd(
    apply_api: bool,
    force: bool,
    kibana_url: str | None,
    quiet: bool,
) -> None:
    """Bootstrap Kibana data views and saved objects.

    \b
    Modes:
        --write-artifacts-only (default):
            Generate NDJSON saved-object files under configs/kibana/.
            Import manually via Kibana UI > Stack Management > Saved Objects.

        --apply:
            Push data views directly to Kibana via the Saved Objects API.
            Requires Kibana to be running and reachable.

    \b
    Examples:
        skill-radar search bootstrap-kibana
        skill-radar search bootstrap-kibana --apply --force
    """
    from skill_radar.config.loader import load_platform_config
    from skill_radar.platform.search.kibana import apply_data_views, write_kibana_artifacts

    init_logging("kibana_bootstrap", enable_file=True)
    config = load_platform_config()

    try:
        if apply_api:
            results = apply_data_views(config.search, kibana_url=kibana_url, force=force)
            if not quiet:
                click.echo("")
                click.echo("  Kibana bootstrap (API):")
                for r in results:
                    click.echo(f"    {r['status']:8s} {r['pattern']}")
                click.echo("")
            errors = [r for r in results if r["status"] == "error"]
            if errors:
                click.echo(f"  {len(errors)} data view(s) failed.")
                finalize_logging()
                sys.exit(EXIT_SEARCH_ERROR)
        else:
            artifact_path = write_kibana_artifacts(config.search)
            if not quiet:
                click.echo("")
                click.echo(f"  Kibana artifacts written to: {artifact_path}")
                click.echo("  Import via Kibana UI > Stack Management > Saved Objects.")
                click.echo("")

        finalize_logging()
        sys.exit(EXIT_SUCCESS)

    except Exception as exc:
        logger.exception("Kibana bootstrap failed")
        click.echo(f"\n  Fatal error: {exc}")
        finalize_logging()
        sys.exit(EXIT_SEARCH_ERROR)
