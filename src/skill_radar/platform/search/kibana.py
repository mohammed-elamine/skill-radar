"""Kibana data view and saved object bootstrap helpers.

Supports two modes:
1. **Write artifacts** — generate NDJSON saved-object files under
   ``configs/kibana/`` for manual import via Kibana UI.
2. **Apply via API** — push data views directly to the Kibana
   Saved Objects API.

Dashboard JSON blobs are stored as versioned artifact files, never
inline in Python code.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from skill_radar.config.models import SearchConfig

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════════════════
# Data view generation
# ═══════════════════════════════════════════════════════════════════════════


def _build_data_view_object(
    *,
    index_pattern: str,
    title: str,
    time_field: str = "ingestion_date",
) -> dict[str, Any]:
    """Build a Kibana data-view saved object dict.

    Parameters
    ----------
    index_pattern:
        Elasticsearch index pattern (e.g. ``skillradar-skill-demand-daily-*``).
    title:
        Human-readable title for the data view.
    time_field:
        Default time field for time-based filtering.
    """
    return {
        "type": "index-pattern",
        "attributes": {
            "title": index_pattern,
            "timeFieldName": time_field,
            "name": title,
        },
    }


def generate_data_views(config: SearchConfig) -> list[dict[str, Any]]:
    """Generate Kibana data view definitions for all served datasets.

    Returns a list of saved-object dicts ready for NDJSON serialization
    or Kibana API import.
    """
    datasets = [
        ("skill-demand-daily", "Skill Demand Daily"),
        ("salary-by-skill-daily", "Salary by Skill Daily"),
        ("occupation-skill-graph", "Occupation-Skill Graph"),
        ("job-skill-matches", "Job-Skill Matches"),
        ("job-occupation-matches", "Job-Occupation Matches"),
    ]

    views: list[dict[str, Any]] = []
    for suffix, title in datasets:
        pattern = f"{config.index_prefix}-{suffix}-*"
        views.append(
            _build_data_view_object(
                index_pattern=pattern,
                title=f"Skill Radar — {title}",
            )
        )
    return views


# ═══════════════════════════════════════════════════════════════════════════
# Artifact writer
# ═══════════════════════════════════════════════════════════════════════════


def write_kibana_artifacts(config: SearchConfig, output_dir: Path | None = None) -> Path:
    """Write Kibana saved-object NDJSON artifact to disk.

    Parameters
    ----------
    config:
        Search configuration.
    output_dir:
        Target directory (defaults to ``configs/kibana``).

    Returns
    -------
    Path
        Path to the generated NDJSON file.
    """
    if output_dir is None:
        output_dir = Path("configs/kibana")
    output_dir.mkdir(parents=True, exist_ok=True)

    views = generate_data_views(config)
    artifact_path = output_dir / "data_views.ndjson"

    with artifact_path.open("w") as fh:
        for view in views:
            fh.write(json.dumps(view, ensure_ascii=False) + "\n")

    logger.info("Kibana artifacts written to %s (%d data views)", artifact_path, len(views))
    return artifact_path


# ═══════════════════════════════════════════════════════════════════════════
# API bootstrap
# ═══════════════════════════════════════════════════════════════════════════


def apply_data_views(
    config: SearchConfig,
    *,
    kibana_url: str | None = None,
    force: bool = False,
) -> list[dict[str, Any]]:
    """Push data view definitions to Kibana via the Saved Objects API.

    Parameters
    ----------
    config:
        Search configuration.
    kibana_url:
        Kibana base URL override. Uses config default if not provided.
    force:
        Overwrite existing data views if True.

    Returns
    -------
    list[dict]
        List of API response dicts per data view.
    """
    url = (kibana_url or config.kibana_url).rstrip("/")
    views = generate_data_views(config)
    results: list[dict[str, Any]] = []

    for view in views:
        pattern = view["attributes"]["title"]
        obj_id = pattern.replace("*", "all").replace(" ", "-").lower()

        endpoint = f"{url}/api/saved_objects/index-pattern/{obj_id}"
        params = {"overwrite": "true"} if force else {}

        try:
            r = requests.post(
                endpoint,
                json={"attributes": view["attributes"]},
                params=params,
                headers={"kbn-xsrf": "true"},
                timeout=config.request_timeout_seconds,
            )
            r.raise_for_status()
            results.append({"id": obj_id, "status": "created", "pattern": pattern})
            logger.info("Data view created: %s → %s", obj_id, pattern)
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 409 and not force:
                results.append({"id": obj_id, "status": "exists", "pattern": pattern})
                logger.info("Data view already exists (use --force to overwrite): %s", obj_id)
            else:
                results.append(
                    {
                        "id": obj_id,
                        "status": "error",
                        "pattern": pattern,
                        "error": str(exc)[:200],
                    }
                )
                logger.warning("Failed to create data view %s: %s", obj_id, exc)
        except Exception as exc:
            results.append(
                {
                    "id": obj_id,
                    "status": "error",
                    "pattern": pattern,
                    "error": str(exc)[:200],
                }
            )
            logger.warning("Failed to create data view %s: %s", obj_id, exc)

    return results


def is_kibana_reachable(kibana_url: str, *, timeout: int = 10) -> bool:
    """Check if Kibana is reachable."""
    try:
        r = requests.get(
            f"{kibana_url.rstrip('/')}/api/status",
            headers={"kbn-xsrf": "true"},
            timeout=timeout,
        )
        return r.status_code == 200
    except Exception:
        return False
