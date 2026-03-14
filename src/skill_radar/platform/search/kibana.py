"""Kibana HTTP client — saved object API operations.

Handles all HTTP communication with the Kibana Saved Objects API.
Asset generation is handled by
:mod:`~skill_radar.platform.search.kibana_assets` and
:mod:`~skill_radar.platform.search.kibana_builders`.
"""

from __future__ import annotations

import io
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

import requests

if TYPE_CHECKING:
    from skill_radar.config.models import SearchConfig

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Health check
# ═══════════════════════════════════════════════════════════════════════════


def is_kibana_reachable(kibana_url: str, *, timeout: int = 10) -> bool:
    """Check if Kibana is reachable via the status API."""
    try:
        r = requests.get(
            f"{kibana_url.rstrip('/')}/api/status",
            headers={"kbn-xsrf": "true"},
            timeout=timeout,
        )
        return r.status_code == 200
    except Exception:
        return False


# ═══════════════════════════════════════════════════════════════════════════
# Saved Objects import API
# ═══════════════════════════════════════════════════════════════════════════


def import_saved_objects(
    ndjson_content: str,
    *,
    kibana_url: str,
    overwrite: bool = True,
    timeout: int = 30,
) -> dict[str, Any]:
    """Import saved objects via Kibana's ``_import`` API.

    Parameters
    ----------
    ndjson_content:
        NDJSON string with saved objects to import.
    kibana_url:
        Kibana base URL (e.g. ``http://localhost:5601``).
    overwrite:
        Overwrite existing objects with the same ID.
    timeout:
        Request timeout in seconds.

    Returns
    -------
    dict
        Kibana import API response body.
    """
    url = f"{kibana_url.rstrip('/')}/api/saved_objects/_import"
    params = {"overwrite": "true"} if overwrite else {}

    # The import API expects multipart form data with the NDJSON as a file
    file_obj = io.BytesIO(ndjson_content.encode("utf-8"))
    files = {"file": ("export.ndjson", file_obj, "application/ndjson")}

    try:
        r = requests.post(
            url,
            files=files,
            params=params,
            headers={"kbn-xsrf": "true"},
            timeout=timeout,
        )
        r.raise_for_status()
        body = r.json()
        logger.info(
            "Kibana import: success=%s, successCount=%d",
            body.get("success"),
            body.get("successCount", 0),
        )
        result: dict[str, Any] = body
        return result
    except Exception as exc:
        logger.error("Kibana import failed: %s", exc)
        raise


def import_ndjson_file(
    ndjson_path: Path,
    *,
    kibana_url: str,
    overwrite: bool = True,
    timeout: int = 30,
) -> dict[str, Any]:
    """Import saved objects from an NDJSON file.

    Parameters
    ----------
    ndjson_path:
        Path to the NDJSON file.
    kibana_url:
        Kibana base URL.
    overwrite:
        Overwrite existing objects.
    timeout:
        Request timeout in seconds.
    """
    content = ndjson_path.read_text(encoding="utf-8")
    return import_saved_objects(
        content,
        kibana_url=kibana_url,
        overwrite=overwrite,
        timeout=timeout,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Saved Objects query API
# ═══════════════════════════════════════════════════════════════════════════


def get_saved_object(
    kibana_url: str,
    obj_type: str,
    obj_id: str,
    *,
    timeout: int = 10,
) -> dict[str, Any] | None:
    """Fetch a single saved object by type and ID.

    Returns None if not found (404).
    """
    url = f"{kibana_url.rstrip('/')}/api/saved_objects/{obj_type}/{obj_id}"
    try:
        r = requests.get(
            url,
            headers={"kbn-xsrf": "true"},
            timeout=timeout,
        )
        if r.status_code == 404:
            return None
        r.raise_for_status()
        result: dict[str, Any] = r.json()
        return result
    except requests.HTTPError:
        return None
    except Exception:
        return None


def find_saved_objects(
    kibana_url: str,
    obj_type: str,
    *,
    search: str = "",
    per_page: int = 100,
    timeout: int = 10,
) -> list[dict[str, Any]]:
    """Search for saved objects by type and optional title search.

    Returns a list of saved object dicts.
    """
    url = f"{kibana_url.rstrip('/')}/api/saved_objects/_find"
    params: dict[str, Any] = {"type": obj_type, "per_page": per_page}
    if search:
        params["search"] = search
        params["search_fields"] = "title"

    try:
        r = requests.get(
            url,
            params=params,
            headers={"kbn-xsrf": "true"},
            timeout=timeout,
        )
        r.raise_for_status()
        body = r.json()
        result: list[dict[str, Any]] = body.get("saved_objects", [])
        return result
    except Exception as exc:
        logger.warning("Failed to find saved objects: %s", exc)
        return []


# ═══════════════════════════════════════════════════════════════════════════
# Legacy data view bootstrap (retained for backwards compatibility)
# ═══════════════════════════════════════════════════════════════════════════


def _build_data_view_object(
    *,
    index_pattern: str,
    title: str,
    time_field: str = "ingestion_date",
) -> dict[str, Any]:
    """Build a Kibana data-view saved object dict (legacy)."""
    return {
        "type": "index-pattern",
        "attributes": {
            "title": index_pattern,
            "timeFieldName": time_field,
            "name": title,
        },
    }


def generate_data_views(config: SearchConfig) -> list[dict[str, Any]]:
    """Generate Kibana data view definitions for all served datasets (legacy).

    Retained for backwards compatibility with existing bootstrap command.
    New code should use :func:`kibana_builders.build_data_views`.
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
                title=f"Skill Radar \u2014 {title}",
            )
        )
    return views


def write_kibana_artifacts(config: SearchConfig, output_dir: Path | None = None) -> Path:
    """Write Kibana saved-object NDJSON artifact to disk (legacy data views).

    Retained for backwards compatibility.  For full dashboard artifacts,
    use :func:`kibana_assets.write_ndjson_artifact`.
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


def apply_data_views(
    config: SearchConfig,
    *,
    kibana_url: str | None = None,
    force: bool = False,
) -> list[dict[str, Any]]:
    """Push data view definitions to Kibana via the Saved Objects API (legacy).

    Retained for backwards compatibility with the existing bootstrap command.
    For full dashboard import, use :func:`import_saved_objects`.
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
            logger.info("Data view created: %s \u2192 %s", obj_id, pattern)
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
