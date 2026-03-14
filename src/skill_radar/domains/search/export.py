"""Search export orchestrator — Gold Iceberg → Elasticsearch.

Reads Gold Iceberg tables, transforms rows into Elasticsearch documents,
and bulk-indexes them with idempotent semantics (partition delete +
re-index + alias swap).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from skill_radar.config.loader import load_platform_config
from skill_radar.domains.search.datasets import (
    PRIMARY_DATASETS,
    ServedDataset,
    get_served_dataset,
    resolve_index_suffix,
)
from skill_radar.domains.search.documents import DOCUMENT_BUILDERS
from skill_radar.platform.lake.layout import LakeLayout
from skill_radar.platform.search.bulk import bulk_index
from skill_radar.platform.search.client import SearchClient
from skill_radar.platform.search.mappings import get_index_body
from skill_radar.platform.search.models import BulkIndexResult, SearchExportResult
from skill_radar.platform.search.naming import build_alias_name, build_index_name

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from skill_radar.config.models import PlatformSettings

logger = logging.getLogger(__name__)


def _read_partition(
    spark: SparkSession,
    table_fqn: str,
    ingestion_date: str,
    country: str,
) -> list[Any]:
    """Read a Gold partition and collect rows."""
    logger.info("Reading partition: %s [%s / %s]", table_fqn, ingestion_date, country)
    df = spark.sql(
        f"SELECT * FROM {table_fqn} "
        f"WHERE ingestion_date = '{ingestion_date}' AND country = '{country}'"
    )
    rows = df.collect()
    logger.info("Read %d rows from %s", len(rows), table_fqn)
    return rows


def _ensure_index(
    client: SearchClient,
    index_name: str,
    dataset: ServedDataset,
    config: PlatformSettings,
) -> None:
    """Create the target index with explicit mappings if missing."""
    if client.index_exists(index_name):
        logger.info("Index already exists: %s", index_name)
        return

    if not config.search.create_index_if_missing:
        raise RuntimeError(
            f"Index '{index_name}' does not exist and create_index_if_missing is disabled."
        )

    body = get_index_body(dataset.mapping_key, config.search)
    client.create_index(index_name, body)
    logger.info("Created index: %s", index_name)


def _delete_partition_docs(
    client: SearchClient,
    index_name: str,
    ingestion_date: str,
    country: str,
) -> None:
    """Delete existing documents for a partition (safe rerun)."""
    if not client.index_exists(index_name):
        return

    query = {
        "bool": {
            "filter": [
                {"term": {"ingestion_date": ingestion_date}},
                {"term": {"country": country}},
            ]
        }
    }
    try:
        result = client.delete_by_query(index_name, query)
        deleted = result.get("deleted", 0)
        logger.info(
            "Deleted %d existing docs for partition [%s/%s] in %s",
            deleted,
            ingestion_date,
            country,
            index_name,
        )
    except Exception as exc:
        logger.warning("Delete-by-query failed (may be first run): %s", exc)


def _update_alias(
    client: SearchClient,
    alias_name: str,
    target_index: str,
) -> None:
    """Point alias to target index, removing old targets atomically."""
    existing = client.get_alias(alias_name)
    actions: list[dict] = []

    # Remove alias from all current indices
    for old_index in existing:
        actions.append({"remove": {"index": old_index, "alias": alias_name}})

    # Add alias to target
    actions.append({"add": {"index": target_index, "alias": alias_name}})

    if actions:
        client.update_aliases(actions)
        logger.info("Alias %s → %s", alias_name, target_index)


def export_dataset(
    spark: SparkSession,
    dataset_name: str,
    *,
    ingestion_date: str,
    country: str,
    config: PlatformSettings,
    client: SearchClient,
    create_index: bool = True,
    refresh: bool = True,
    alias_swap: bool = True,
    dry_run: bool = False,
) -> BulkIndexResult:
    """Export one Gold dataset to Elasticsearch.

    Parameters
    ----------
    spark:
        Active Spark session.
    dataset_name:
        Name of the served dataset (from registry).
    ingestion_date:
        Partition date in YYYY-MM-DD format.
    country:
        Country code (e.g. ``fr``).
    config:
        Platform configuration.
    client:
        Elasticsearch HTTP client.
    create_index:
        Create index with mappings if missing.
    refresh:
        Force index refresh after bulk load.
    alias_swap:
        Update alias to point to new index.
    dry_run:
        If True, read and transform but do not index.

    Returns
    -------
    BulkIndexResult
        Result of the bulk indexing operation.
    """
    dataset = get_served_dataset(dataset_name)
    layout = LakeLayout(config)
    index_suffix = resolve_index_suffix(dataset, config.search)

    # Resolve Gold table FQN
    fqn_fn = getattr(layout, dataset.fqn_method)
    table_fqn = fqn_fn()

    # Build index names
    index_name = build_index_name(index_suffix, ingestion_date, country, config.search)
    alias_name = build_alias_name(index_suffix, country, config.search)

    logger.info(
        "Exporting %s → %s (alias: %s)",
        dataset_name,
        index_name,
        alias_name,
    )

    # Read Gold partition
    rows = _read_partition(spark, table_fqn, ingestion_date, country)

    if not rows:
        logger.warning("No rows found for %s [%s/%s]", dataset_name, ingestion_date, country)
        return BulkIndexResult(index_name=index_name, total_documents=0)

    # Transform to documents
    builder_fn = DOCUMENT_BUILDERS[dataset.document_builder]
    documents = builder_fn(rows)
    logger.info("Built %d documents for %s", len(documents), dataset_name)

    if dry_run:
        logger.info("Dry run — skipping indexing for %s (%d docs)", dataset_name, len(documents))
        return BulkIndexResult(
            index_name=index_name,
            total_documents=len(documents),
            success_count=len(documents),
        )

    # Ensure index exists with correct mappings
    if create_index:
        _ensure_index(client, index_name, dataset, config)

    # Delete existing partition docs for idempotent rerun
    _delete_partition_docs(client, index_name, ingestion_date, country)

    # Bulk index
    result = bulk_index(client, index_name, documents, config.search)

    # Refresh
    if refresh:
        client.refresh_index(index_name)

    # Alias swap
    if alias_swap:
        _update_alias(client, alias_name, index_name)

    return result


def run_search_export(
    spark: SparkSession,
    *,
    ingestion_date: str,
    country: str,
    datasets: list[str] | None = None,
    config: PlatformSettings | None = None,
    es_url: str | None = None,
    create_index: bool = True,
    refresh: bool = True,
    alias_swap: bool = True,
    dry_run: bool = False,
    run_id: str = "",
) -> SearchExportResult:
    """Run the full search export pipeline.

    Parameters
    ----------
    spark:
        Active Spark session.
    ingestion_date:
        Partition date.
    country:
        Country code.
    datasets:
        List of dataset names to export. Defaults to primary datasets.
    config:
        Platform config (loaded from defaults if None).
    es_url:
        Elasticsearch URL override.
    create_index:
        Create indices if missing.
    refresh:
        Refresh after bulk load.
    alias_swap:
        Update aliases.
    dry_run:
        Transform only, no indexing.
    run_id:
        Logging run identifier.

    Returns
    -------
    SearchExportResult
        Aggregated result across all datasets.
    """
    if config is None:
        config = load_platform_config()

    target_datasets = datasets or PRIMARY_DATASETS

    # Resolve Elasticsearch URL
    url = es_url or config.search.elasticsearch_url
    client = SearchClient(url, timeout=config.search.request_timeout_seconds)

    result = SearchExportResult(run_id=run_id)

    logger.info(
        "Search export starting: datasets=%s, date=%s, country=%s, es=%s",
        target_datasets,
        ingestion_date,
        country,
        url,
    )

    for ds_name in target_datasets:
        try:
            ds_result = export_dataset(
                spark,
                ds_name,
                ingestion_date=ingestion_date,
                country=country,
                config=config,
                client=client,
                create_index=create_index,
                refresh=refresh,
                alias_swap=alias_swap,
                dry_run=dry_run,
            )
            result.results[ds_name] = ds_result
            result.datasets_exported.append(ds_name)

            if not ds_result.success:
                result.success = False
                logger.error(
                    "Export failed for %s: %d errors",
                    ds_name,
                    ds_result.error_count,
                )

        except Exception as exc:
            logger.exception("Export failed for %s", ds_name)
            result.success = False
            result.error = f"{ds_name}: {exc!s}"
            result.results[ds_name] = BulkIndexResult(
                index_name=ds_name,
                error_count=1,
                errors=[str(exc)[:200]],
            )

    logger.info(
        "Search export complete: success=%s, datasets=%s",
        result.success,
        result.datasets_exported,
    )
    return result
