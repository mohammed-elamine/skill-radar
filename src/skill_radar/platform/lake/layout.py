"""Lake path builder — single source of truth for all storage paths.

Every module that needs to compute a storage path **must** go through this
class.  No path string should be constructed elsewhere in the codebase.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from skill_radar.config.models import PlatformSettings

from .enums import LakeLayer


class LakeLayout:
    """Centralised path builder for the data lake.

    Parameters
    ----------
    config:
        Platform configuration containing lake root prefix and layer names.
    """

    def __init__(self, config: PlatformSettings) -> None:
        self._root = config.lake.root_prefix
        self._layers = config.lake.layers

    # -- helpers ---------------------------------------------------------

    def _layer_name(self, layer: LakeLayer) -> str:
        """Resolve a ``LakeLayer`` enum to its configured directory name."""
        value = getattr(self._layers, layer.value, None)
        if not isinstance(value, str) or not value:
            raise ValueError(f"Invalid layer mapping for {layer!r}: {value!r}")
        return cast("str", value)

    # -- public API ------------------------------------------------------

    def landing_artifact(
        self,
        domain: str,
        source: str,
        version: str,
        lang: str,
    ) -> str:
        """Return the object-key prefix for a landing artifact.

        Example::

            data/landing/taxonomy/esco/artifact/version=v1.2.1/lang=fr
        """
        return (
            f"{self._root}/{self._layer_name(LakeLayer.LANDING)}"
            f"/{domain}/{source}/artifact"
            f"/version={version}/lang={lang}"
        )

    def landing_zip_key(
        self,
        domain: str,
        source: str,
        version: str,
        lang: str,
    ) -> str:
        """Full S3 key for the landing ZIP artifact.

        Example::

            data/landing/taxonomy/esco/artifact/version=v1.2.1/lang=fr/esco.zip
        """
        return f"{self.landing_artifact(domain, source, version, lang)}/{source}.zip"

    def landing_manifest_key(
        self,
        domain: str,
        source: str,
        version: str,
        lang: str,
    ) -> str:
        """Full S3 key for the landing manifest.

        Example::

            data/landing/taxonomy/esco/artifact/version=v1.2.1/lang=fr/manifest.json
        """
        return f"{self.landing_artifact(domain, source, version, lang)}/manifest.json"

    def bronze_staging_prefix(
        self,
        domain: str,
        source: str,
        version: str,
        lang: str,
        run_id: str,
    ) -> str:
        """S3 prefix for staging extracted CSVs before Spark reads them.

        Example::

            data/bronze/taxonomy/esco/staging/version=v1.2.0/lang=fr/run_id=abc123
        """
        return (
            f"{self._root}/{self._layer_name(LakeLayer.BRONZE)}"
            f"/{domain}/{source}/staging"
            f"/version={version}/lang={lang}/run_id={run_id}"
        )

    def iceberg_table_fqn(
        self,
        layer: str,
        dataset: str,
        entity: str,
        *,
        catalog: str = "sr",
    ) -> str:
        """Fully-qualified Iceberg table name.

        Example::

            sr.sr_bronze.esco_skills_raw
        """
        namespace = f"sr_{layer}"
        return f"{catalog}.{namespace}.{dataset}_{entity}_raw"

    def iceberg_namespace(
        self,
        layer: str,
        *,
        catalog: str = "sr",
    ) -> str:
        """Fully-qualified Iceberg namespace.

        Example::

            sr.sr_bronze
        """
        return f"{catalog}.sr_{layer}"

    def layer_prefix(
        self,
        layer: LakeLayer,
        domain: str,
        source: str,
        entity: str,
    ) -> str:
        """Return the base prefix for a layer/domain/source/entity path.

        Example::

            data/bronze/taxonomy/esco/skills
        """
        return f"{self._root}/{self._layer_name(layer)}/{domain}/{source}/{entity}"
