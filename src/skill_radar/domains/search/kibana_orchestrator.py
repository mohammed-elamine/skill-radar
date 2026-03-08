"""Kibana dashboard orchestrator — generate, apply, export, validate.

High-level workflow for managing Kibana assets:

- **generate**: Build NDJSON artifacts from code (deterministic)
- **apply**: Push generated assets to Kibana via import API
- **bootstrap**: ensure data views + dashboards exist in Kibana

This orchestrator delegates to:

- :mod:`~skill_radar.platform.search.kibana_assets` for generation
- :mod:`~skill_radar.platform.search.kibana` for API communication
- :mod:`~skill_radar.domains.search.kibana_metadata` for metadata
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from skill_radar.domains.search.kibana_metadata import validate_all_metadata
from skill_radar.platform.search.kibana import (
    import_saved_objects,
    is_kibana_reachable,
)
from skill_radar.platform.search.kibana_assets import (
    KibanaAssetResult,
    generate_all_assets,
    objects_to_ndjson,
    write_ndjson_artifact,
)

if TYPE_CHECKING:
    from pathlib import Path

    from skill_radar.config.models import PlatformSettings

logger = logging.getLogger(__name__)


def generate_kibana_assets(
    config: PlatformSettings,
    *,
    output_dir: Path | None = None,
    filename: str = "skill_radar_dashboards.ndjson",
) -> KibanaAssetResult:
    """Generate Kibana dashboard NDJSON artifact.

    Validates metadata against mappings, builds all saved objects,
    and writes the NDJSON file to disk.

    Parameters
    ----------
    config:
        Platform settings.
    output_dir:
        Target directory (defaults to ``configs/kibana``).
    filename:
        Output filename.

    Returns
    -------
    KibanaAssetResult
        Result with counts and artifact path.

    Raises
    ------
    ValueError
        If metadata validation fails.
    """
    # Phase 1: Validate metadata
    errors = validate_all_metadata()
    if errors:
        raise ValueError(
            "Kibana metadata validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        )

    # Phase 2: Generate and write
    result = write_ndjson_artifact(
        config.search,
        output_dir=output_dir,
        filename=filename,
    )

    logger.info(
        "Kibana assets generated: %d objects → %s",
        result.total_objects,
        result.artifact_path,
    )
    return result


def apply_kibana_assets(
    config: PlatformSettings,
    *,
    kibana_url: str | None = None,
    overwrite: bool = True,
    dry_run: bool = False,
    output_dir: Path | None = None,
) -> KibanaAssetResult:
    """Generate and apply Kibana assets to a running Kibana instance.

    Performs:
    1. Metadata validation
    2. Asset generation
    3. Kibana reachability check
    4. Import via saved objects API

    Parameters
    ----------
    config:
        Platform settings.
    kibana_url:
        Kibana URL override.
    overwrite:
        Overwrite existing saved objects.
    dry_run:
        Generate only, don't push to Kibana.
    output_dir:
        Also write NDJSON artifact to disk if set.

    Returns
    -------
    KibanaAssetResult
        Result with counts and status.
    """
    resolved_url = (kibana_url or config.search.kibana_url).rstrip("/")

    # Phase 1: Validate metadata
    errors = validate_all_metadata()
    if errors:
        raise ValueError(
            "Kibana metadata validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        )

    # Phase 2: Generate
    objects = generate_all_assets(config.search)
    ndjson = objects_to_ndjson(objects)

    dv_count = sum(1 for o in objects if o.type == "index-pattern")
    vis_count = sum(1 for o in objects if o.type == "lens")
    dash_count = sum(1 for o in objects if o.type == "dashboard")
    ss_count = sum(1 for o in objects if o.type == "search")

    result = KibanaAssetResult(
        data_views_count=dv_count,
        visualizations_count=vis_count,
        dashboards_count=dash_count,
        saved_searches_count=ss_count,
        total_objects=len(objects),
    )

    # Phase 2b: Write artifact if output_dir provided
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = output_dir / "skill_radar_dashboards.ndjson"
        artifact_path.write_text(ndjson, encoding="utf-8")
        result.artifact_path = str(artifact_path)
        logger.info("NDJSON artifact written to %s", artifact_path)

    # Phase 3: Dry-run exit
    if dry_run:
        logger.info(
            "Dry run: %d objects generated (%d DV, %d vis, %d dash, %d SS)",
            len(objects),
            dv_count,
            vis_count,
            dash_count,
            ss_count,
        )
        return result

    # Phase 4: Check Kibana reachability
    if not is_kibana_reachable(resolved_url, timeout=config.search.request_timeout_seconds):
        result.errors.append(f"Kibana not reachable at {resolved_url}")
        return result

    # Phase 5: Import
    try:
        import_result = import_saved_objects(
            ndjson,
            kibana_url=resolved_url,
            overwrite=overwrite,
            timeout=config.search.request_timeout_seconds,
        )

        success = import_result.get("success", False)
        success_count = import_result.get("successCount", 0)

        if success:
            result.applied = True
            logger.info(
                "Kibana assets applied: %d objects imported to %s",
                success_count,
                resolved_url,
            )
        else:
            # Collect errors from the response
            for err in import_result.get("errors", []):
                err_msg = (
                    f"{err.get('type', '?')}/{err.get('id', '?')}: "
                    f"{err.get('error', {}).get('message', 'unknown error')}"
                )
                result.errors.append(err_msg)
            logger.warning(
                "Kibana import partial failure: %d succeeded, %d errors",
                success_count,
                len(result.errors),
            )

    except Exception as exc:
        result.errors.append(str(exc)[:500])
        logger.error("Kibana import failed: %s", exc)

    return result


def bootstrap_kibana(
    config: PlatformSettings,
    *,
    kibana_url: str | None = None,
    overwrite: bool = True,
) -> KibanaAssetResult:
    """Ensure data views and dashboards exist in Kibana.

    Convenience wrapper that generates and applies all assets.
    Equivalent to ``apply_kibana_assets`` with ``dry_run=False``.
    """
    return apply_kibana_assets(
        config,
        kibana_url=kibana_url,
        overwrite=overwrite,
    )
