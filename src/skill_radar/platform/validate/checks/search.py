"""Search-layer validation checks for Elasticsearch indices and Kibana.

Health checks
- Elasticsearch reachable
- Cluster health ≥ yellow
- Kibana reachable

Per-index checks
- Index/alias exists
- Mapping contains critical fields with expected types
- Document count > 0 for served partition
- Required fields are populated (no null/empty on key identifiers)

Consistency checks
- Gold partition count vs indexed document count
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from skill_radar.domains.search.datasets import (
    PRIMARY_DATASETS,
    ServedDataset,
    get_served_dataset,
    resolve_index_suffix,
)
from skill_radar.platform.search.client import SearchClient
from skill_radar.platform.search.kibana import is_kibana_reachable
from skill_radar.platform.search.mappings import get_mapping
from skill_radar.platform.search.naming import build_alias_name, build_index_name
from skill_radar.platform.validate.models import CheckResult, NamedCheck, create_check

if TYPE_CHECKING:
    from skill_radar.config.models import PlatformSettings

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Infrastructure health checks
# ═══════════════════════════════════════════════════════════════════════════


def _check_es_reachable(client: SearchClient) -> CheckResult:
    """Check that Elasticsearch is reachable."""
    try:
        reachable = client.is_reachable()
        return create_check(
            name="search.es.reachable",
            description="Elasticsearch is reachable",
            passed=reachable,
            detail="OK" if reachable else "Connection failed",
        )
    except Exception as exc:
        return create_check(
            name="search.es.reachable",
            description="Elasticsearch is reachable",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_es_cluster_health(client: SearchClient) -> CheckResult:
    """Check that cluster health is at least yellow."""
    try:
        status = client.cluster_status()
        passed = status in ("green", "yellow")
        return create_check(
            name="search.es.cluster_health",
            description="Cluster health is at least yellow",
            passed=passed,
            detail=f"status={status}",
        )
    except Exception as exc:
        return create_check(
            name="search.es.cluster_health",
            description="Cluster health is at least yellow",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_kibana_reachable(kibana_url: str) -> CheckResult:
    """Check that Kibana is reachable."""
    try:
        reachable = is_kibana_reachable(kibana_url)
        return create_check(
            name="search.kibana.reachable",
            description="Kibana is reachable",
            passed=reachable,
            detail="OK" if reachable else "Connection failed",
        )
    except Exception as exc:
        return create_check(
            name="search.kibana.reachable",
            description="Kibana is reachable",
            passed=False,
            detail=str(exc)[:200],
        )


# ═══════════════════════════════════════════════════════════════════════════
# Per-index checks
# ═══════════════════════════════════════════════════════════════════════════


def _check_index_exists(
    client: SearchClient,
    index_name: str,
    dataset_name: str,
) -> CheckResult:
    """Check that an index or alias exists."""
    try:
        exists = client.index_exists(index_name)
        return create_check(
            name=f"search.{dataset_name}.index_exists",
            description=f"{dataset_name} index exists: {index_name}",
            passed=exists,
            detail="exists" if exists else "missing",
        )
    except Exception as exc:
        return create_check(
            name=f"search.{dataset_name}.index_exists",
            description=f"{dataset_name} index exists: {index_name}",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_mapping_fields(
    client: SearchClient,
    index_name: str,
    dataset: ServedDataset,
    _config: Any,
) -> CheckResult:
    """Check that mapping contains critical fields with expected types."""
    ds_name = dataset.name
    try:
        mapping_resp = client.get_mapping(index_name)
        # Navigate to the properties — response shape:
        # { "index_name": { "mappings": { "properties": { ... } } } }
        index_mapping: dict[str, Any] = next(iter(mapping_resp.values()), {})
        actual_props = index_mapping.get("mappings", {}).get("properties", {})

        expected = get_mapping(dataset.mapping_key).get("properties", {})
        missing: list[str] = []
        type_mismatches: list[str] = []

        for field_name, expected_def in expected.items():
            if field_name not in actual_props:
                missing.append(field_name)
            elif actual_props[field_name].get("type") != expected_def.get("type"):
                type_mismatches.append(
                    f"{field_name}: expected={expected_def.get('type')}, "
                    f"actual={actual_props[field_name].get('type')}"
                )

        issues = missing + type_mismatches
        return create_check(
            name=f"search.{ds_name}.mapping_fields",
            description=f"{ds_name} mapping contains expected fields",
            passed=len(issues) == 0,
            detail=f"missing={missing}, mismatches={type_mismatches}" if issues else "OK",
            metrics={"missing": len(missing), "type_mismatches": len(type_mismatches)},
        )
    except Exception as exc:
        return create_check(
            name=f"search.{ds_name}.mapping_fields",
            description=f"{ds_name} mapping contains expected fields",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_doc_count(
    client: SearchClient,
    index_name: str,
    dataset_name: str,
    ingestion_date: str,
    country: str,
) -> CheckResult:
    """Check that the index has documents for the served partition."""
    try:
        # Query for specific partition
        result = client.search(
            index_name,
            {
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"ingestion_date": ingestion_date}},
                            {"term": {"country": country}},
                        ]
                    }
                },
            },
        )
        count = result.get("hits", {}).get("total", {}).get("value", 0)
        return create_check(
            name=f"search.{dataset_name}.doc_count",
            description=f"{dataset_name} has documents for [{ingestion_date}/{country}]",
            passed=count > 0,
            detail=f"count={count}",
            metrics={"doc_count": count},
        )
    except Exception as exc:
        return create_check(
            name=f"search.{dataset_name}.doc_count",
            description=f"{dataset_name} has documents",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_required_fields_populated(
    client: SearchClient,
    index_name: str,
    dataset_name: str,
    required_fields: list[str],
) -> CheckResult:
    """Check that key identifier fields are not null/empty (sample-based)."""
    try:
        # Sample first 10 docs and check key fields
        result = client.search(index_name, {"size": 10})
        hits = result.get("hits", {}).get("hits", [])

        if not hits:
            return create_check(
                name=f"search.{dataset_name}.required_fields",
                description=f"{dataset_name} required fields populated",
                passed=False,
                detail="no documents found",
            )

        empty_fields: list[str] = []
        for field in required_fields:
            for hit in hits:
                val = hit.get("_source", {}).get(field)
                if val is None or val == "":
                    empty_fields.append(field)
                    break

        return create_check(
            name=f"search.{dataset_name}.required_fields",
            description=f"{dataset_name} required fields populated (sample of {len(hits)})",
            passed=len(empty_fields) == 0,
            detail=f"empty_fields={empty_fields}" if empty_fields else "OK",
        )
    except Exception as exc:
        return create_check(
            name=f"search.{dataset_name}.required_fields",
            description=f"{dataset_name} required fields populated",
            passed=False,
            detail=str(exc)[:200],
        )


# ═══════════════════════════════════════════════════════════════════════════
# Check factory
# ═══════════════════════════════════════════════════════════════════════════

# Key identifier fields per dataset (for required-field validation)
_KEY_FIELDS: dict[str, list[str]] = {
    "skill_demand_daily": ["doc_id", "ingestion_date", "country", "esco_skill_concept_uri"],
    "salary_by_skill_daily": ["doc_id", "ingestion_date", "country", "esco_skill_concept_uri"],
    "occupation_skill_graph": [
        "doc_id",
        "ingestion_date",
        "country",
        "esco_occupation_concept_uri",
        "esco_skill_concept_uri",
    ],
    "job_skill_matches": ["doc_id", "ingestion_date", "country", "job_id"],
    "job_occupation_matches": ["doc_id", "ingestion_date", "country", "job_id"],
}


def get_search_infra_checks(
    config: PlatformSettings,
    *,
    es_url: str | None = None,
    kibana_url: str | None = None,
) -> list[NamedCheck]:
    """Return infrastructure-level search checks.

    Parameters
    ----------
    config:
        Platform configuration.
    es_url:
        Elasticsearch URL override.
    kibana_url:
        Kibana URL override (for Docker-side validation).
    """
    url = es_url or config.search.elasticsearch_url
    client = SearchClient(url, timeout=config.search.request_timeout_seconds)
    resolved_kibana_url = kibana_url or config.search.kibana_url

    return [
        NamedCheck(
            name="search.es.reachable",
            description="Elasticsearch is reachable",
            fn=lambda: _check_es_reachable(client),
        ),
        NamedCheck(
            name="search.es.cluster_health",
            description="Cluster health ≥ yellow",
            fn=lambda: _check_es_cluster_health(client),
        ),
        NamedCheck(
            name="search.kibana.reachable",
            description="Kibana is reachable",
            fn=lambda: _check_kibana_reachable(resolved_kibana_url),
        ),
    ]


def get_search_index_checks(
    config: PlatformSettings,
    *,
    ingestion_date: str,
    country: str,
    datasets: list[str] | None = None,
    es_url: str | None = None,
) -> list[NamedCheck]:
    """Return per-index validation checks for served datasets.

    Parameters
    ----------
    config:
        Platform configuration.
    ingestion_date:
        Partition date to validate.
    country:
        Country code.
    datasets:
        Dataset names to validate. Defaults to primary datasets.
    es_url:
        Elasticsearch URL override.
    """
    url = es_url or config.search.elasticsearch_url
    client = SearchClient(url, timeout=config.search.request_timeout_seconds)
    target_datasets = datasets or PRIMARY_DATASETS
    checks: list[NamedCheck] = []

    for ds_name in target_datasets:
        dataset = get_served_dataset(ds_name)
        suffix = resolve_index_suffix(dataset, config.search)
        index_name = build_index_name(suffix, ingestion_date, country, config.search)
        alias_name = build_alias_name(suffix, country, config.search)
        key_fields = _KEY_FIELDS.get(ds_name, ["doc_id"])

        # Use alias for checks (more stable)
        target = alias_name

        # Capture loop variables properly
        def _make_checks(
            _client: SearchClient = client,
            _target: str = target,
            _index: str = index_name,
            _dataset: ServedDataset = dataset,
            _ds_name: str = ds_name,
            _key_fields: list[str] = key_fields,
            _ingestion_date: str = ingestion_date,
            _country: str = country,
            _config: PlatformSettings = config,
        ) -> list[NamedCheck]:
            return [
                NamedCheck(
                    name=f"search.{_ds_name}.index_exists",
                    description=f"{_ds_name} alias exists: {_target}",
                    fn=lambda: _check_index_exists(_client, _target, _ds_name),
                ),
                NamedCheck(
                    name=f"search.{_ds_name}.mapping_fields",
                    description=f"{_ds_name} mapping contains expected fields",
                    fn=lambda: _check_mapping_fields(_client, _target, _dataset, _config),
                ),
                NamedCheck(
                    name=f"search.{_ds_name}.doc_count",
                    description=f"{_ds_name} docs for [{_ingestion_date}/{_country}]",
                    fn=lambda: _check_doc_count(
                        _client, _target, _ds_name, _ingestion_date, _country
                    ),
                ),
                NamedCheck(
                    name=f"search.{_ds_name}.required_fields",
                    description=f"{_ds_name} required fields populated",
                    fn=lambda: _check_required_fields_populated(
                        _client, _target, _ds_name, _key_fields
                    ),
                ),
            ]

        checks.extend(_make_checks())

    return checks


def get_search_checks(
    config: PlatformSettings,
    *,
    ingestion_date: str,
    country: str,
    datasets: list[str] | None = None,
    es_url: str | None = None,
    kibana_url: str | None = None,
) -> list[NamedCheck]:
    """Return all search validation checks (infra + per-index).

    Parameters
    ----------
    config:
        Platform configuration.
    ingestion_date:
        Partition date to validate.
    country:
        Country code.
    datasets:
        Dataset names to validate.
    es_url:
        Elasticsearch URL override.
    kibana_url:
        Kibana URL override (for Docker-side validation).
    """
    infra_checks = get_search_infra_checks(config, es_url=es_url, kibana_url=kibana_url)
    index_checks = get_search_index_checks(
        config,
        ingestion_date=ingestion_date,
        country=country,
        datasets=datasets,
        es_url=es_url,
    )
    kibana_checks = get_kibana_asset_checks(config, kibana_url=kibana_url)
    return infra_checks + index_checks + kibana_checks


# ═══════════════════════════════════════════════════════════════════════════
# Kibana asset validation checks
# ═══════════════════════════════════════════════════════════════════════════


def _check_kibana_data_view_exists(
    kibana_url: str,
    data_view_id: str,
    data_view_title: str,
    *,
    timeout: int = 10,
) -> CheckResult:
    """Check that an expected Kibana data view exists."""
    from skill_radar.platform.search.kibana import get_saved_object

    try:
        obj = get_saved_object(kibana_url, "index-pattern", data_view_id, timeout=timeout)
        if obj is None:
            return create_check(
                name=f"search.kibana.dv.{data_view_id}",
                description=f"Data view exists: {data_view_id}",
                passed=False,
                detail="missing",
            )
        actual_title = obj.get("attributes", {}).get("name", "")
        if actual_title != data_view_title:
            return create_check(
                name=f"search.kibana.dv.{data_view_id}",
                description=f"Data view exists: {data_view_id}",
                passed=True,
                warn=True,
                warn_reason=f"Title mismatch: expected='{data_view_title}', actual='{actual_title}'",
            )
        return create_check(
            name=f"search.kibana.dv.{data_view_id}",
            description=f"Data view exists: {data_view_id}",
            passed=True,
            detail="exists",
        )
    except Exception as exc:
        return create_check(
            name=f"search.kibana.dv.{data_view_id}",
            description=f"Data view exists: {data_view_id}",
            passed=False,
            detail=str(exc)[:200],
        )


def _check_kibana_dashboard_exists(
    kibana_url: str,
    dashboard_id: str,
    dashboard_title: str,
    *,
    timeout: int = 10,
) -> CheckResult:
    """Check that an expected Kibana dashboard exists."""
    from skill_radar.platform.search.kibana import get_saved_object

    try:
        obj = get_saved_object(kibana_url, "dashboard", dashboard_id, timeout=timeout)
        if obj is None:
            return create_check(
                name=f"search.kibana.dash.{dashboard_id}",
                description=f"Dashboard exists: {dashboard_title}",
                passed=False,
                detail="missing",
            )
        actual_title = obj.get("attributes", {}).get("title", "")
        if actual_title != dashboard_title:
            return create_check(
                name=f"search.kibana.dash.{dashboard_id}",
                description=f"Dashboard exists: {dashboard_title}",
                passed=True,
                warn=True,
                warn_reason=f"Title mismatch: expected='{dashboard_title}', actual='{actual_title}'",
            )
        return create_check(
            name=f"search.kibana.dash.{dashboard_id}",
            description=f"Dashboard exists: {dashboard_title}",
            passed=True,
            detail="exists",
        )
    except Exception as exc:
        return create_check(
            name=f"search.kibana.dash.{dashboard_id}",
            description=f"Dashboard exists: {dashboard_title}",
            passed=False,
            detail=str(exc)[:200],
        )


def get_kibana_asset_checks(
    config: PlatformSettings,
    *,
    kibana_url: str | None = None,
) -> list[NamedCheck]:
    """Return validation checks for expected Kibana data views and dashboards.

    These checks verify that the expected code-managed assets have been
    applied to Kibana (data views, dashboards).
    """
    from skill_radar.domains.search.kibana_metadata import DASHBOARD_DATASETS
    from skill_radar.platform.search.kibana_assets import (
        EXPECTED_DASHBOARDS,
        get_expected_data_view_ids,
    )

    resolved_kibana_url = kibana_url or config.search.kibana_url
    timeout = config.search.request_timeout_seconds
    checks: list[NamedCheck] = []

    # Data view checks
    expected_dv_ids = get_expected_data_view_ids(config.search)
    for dv_id in expected_dv_ids:
        # Extract suffix from ID to find the metadata
        suffix = dv_id.replace(f"{config.search.index_prefix}-dv-", "")
        meta = None
        for m in DASHBOARD_DATASETS.values():
            if m.index_suffix == suffix:
                meta = m
                break
        dv_title = meta.data_view_title if meta else dv_id

        def _make_dv_check(
            _url: str = resolved_kibana_url,
            _id: str = dv_id,
            _title: str = dv_title,
            _timeout: int = timeout,
        ) -> NamedCheck:
            return NamedCheck(
                name=f"search.kibana.dv.{_id}",
                description=f"Data view exists: {_title}",
                fn=lambda: _check_kibana_data_view_exists(_url, _id, _title, timeout=_timeout),
            )

        checks.append(_make_dv_check())

    # Dashboard checks
    for dash in EXPECTED_DASHBOARDS:

        def _make_dash_check(
            _url: str = resolved_kibana_url,
            _id: str = dash["id"],
            _title: str = dash["title"],
            _timeout: int = timeout,
        ) -> NamedCheck:
            return NamedCheck(
                name=f"search.kibana.dash.{_id}",
                description=f"Dashboard exists: {_title}",
                fn=lambda: _check_kibana_dashboard_exists(_url, _id, _title, timeout=_timeout),
            )

        checks.append(_make_dash_check())

    return checks
