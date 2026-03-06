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
        self._config = config
        self._root = config.lake.root_prefix
        self._layers = config.lake.layers

    @property
    def iceberg_catalog(self) -> str:
        """Return the configured Iceberg catalog name."""
        return self._config.storage.iceberg.catalog_name

    @property
    def iceberg_namespace_prefix(self) -> str:
        """Return the configured Iceberg namespace prefix."""
        return self._config.storage.iceberg.namespace_prefix

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
        catalog: str | None = None,
        raw: bool = True,
    ) -> str:
        """Fully-qualified Iceberg table name.

        Parameters
        ----------
        layer:
            Lake layer (e.g. "bronze", "silver", "gold").
        dataset:
            Dataset name (e.g. "esco").
        entity:
            Entity name (e.g. "skills", "occupations", "relations").
        catalog:
            Optional catalog override.
        raw:
            If True (default), append ``_raw`` suffix (Bronze convention).
            If False, use clean entity name (Silver/Gold convention).

        Examples
        --------
        Bronze (raw=True, default)::

            sr.sr_bronze.esco_skills_raw

        Silver (raw=False)::

            sr.sr_silver.esco_skills
        """
        cat = catalog or self.iceberg_catalog
        ns_prefix = self.iceberg_namespace_prefix
        namespace = f"{ns_prefix}_{layer}"
        suffix = "_raw" if raw else ""
        return f"{cat}.{namespace}.{dataset}_{entity}{suffix}"

    def iceberg_namespace(
        self,
        layer: str,
        *,
        catalog: str | None = None,
    ) -> str:
        """Fully-qualified Iceberg namespace.

        Example::

            sr.sr_bronze
        """
        cat = catalog or self.iceberg_catalog
        return f"{cat}.{self.iceberg_namespace_name(layer)}"

    def iceberg_namespace_name(
        self,
        layer: str,
    ) -> str:
        """Return just the namespace name (without catalog prefix).

        Example::

            sr_bronze
        """
        ns_prefix = self.iceberg_namespace_prefix
        if not ns_prefix:
            return layer
        return f"{ns_prefix}_{layer}"

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

    # -- Adzuna convenience helpers ----------------------------------------

    def adzuna_bronze_jobs_raw_fqn(self, *, catalog: str | None = None) -> str:
        """FQN for the Adzuna Bronze raw jobs table.

        Example::

            sr.sr_bronze.adzuna_jobs_raw
        """
        return self.iceberg_table_fqn("bronze", "adzuna", "jobs", catalog=catalog, raw=True)

    def adzuna_bronze_request_log_fqn(self, *, catalog: str | None = None) -> str:
        """FQN for the Adzuna Bronze request log table.

        Example::

            sr.sr_bronze.adzuna_request_log_raw
        """
        return self.iceberg_table_fqn("bronze", "adzuna", "request_log", catalog=catalog, raw=True)

    def adzuna_silver_jobs_fqn(self, *, catalog: str | None = None) -> str:
        """FQN for the Adzuna Silver jobs table.

        Example::

            sr.sr_silver.adzuna_jobs
        """
        return self.iceberg_table_fqn("silver", "adzuna", "jobs", catalog=catalog, raw=False)

    # -- ESCO Silver convenience helpers -----------------------------------

    def esco_silver_skills_fqn(self, *, catalog: str | None = None) -> str:
        """FQN for the ESCO Silver skills table.

        Example::

            sr.sr_silver.esco_skills
        """
        return self.iceberg_table_fqn("silver", "esco", "skills", catalog=catalog, raw=False)

    def esco_silver_occupations_fqn(self, *, catalog: str | None = None) -> str:
        """FQN for the ESCO Silver occupations table.

        Example::

            sr.sr_silver.esco_occupations
        """
        return self.iceberg_table_fqn("silver", "esco", "occupations", catalog=catalog, raw=False)

    def esco_silver_relations_fqn(self, *, catalog: str | None = None) -> str:
        """FQN for the ESCO Silver relations table.

        Example::

            sr.sr_silver.esco_relations
        """
        return self.iceberg_table_fqn("silver", "esco", "relations", catalog=catalog, raw=False)

    # -- Gold convenience helpers ------------------------------------------

    def gold_job_skill_matches_fqn(self, *, catalog: str | None = None) -> str:
        """FQN for the Gold job-skill matches table.

        Example::

            sr.sr_gold.gold_job_skill_matches
        """
        return self.iceberg_table_fqn(
            "gold", "gold", "job_skill_matches", catalog=catalog, raw=False
        )

    def gold_job_occupation_matches_fqn(self, *, catalog: str | None = None) -> str:
        """FQN for the Gold job-occupation matches table.

        Example::

            sr.sr_gold.gold_job_occupation_matches
        """
        return self.iceberg_table_fqn(
            "gold", "gold", "job_occupation_matches", catalog=catalog, raw=False
        )

    def gold_skill_demand_daily_fqn(self, *, catalog: str | None = None) -> str:
        """FQN for the Gold daily skill demand KPI table.

        Example::

            sr.sr_gold.gold_skill_demand_daily
        """
        return self.iceberg_table_fqn(
            "gold", "gold", "skill_demand_daily", catalog=catalog, raw=False
        )

    def gold_salary_by_skill_daily_fqn(self, *, catalog: str | None = None) -> str:
        """FQN for the Gold daily salary-by-skill KPI table.

        Example::

            sr.sr_gold.gold_salary_by_skill_daily
        """
        return self.iceberg_table_fqn(
            "gold", "gold", "salary_by_skill_daily", catalog=catalog, raw=False
        )

    def gold_occupation_skill_graph_fqn(self, *, catalog: str | None = None) -> str:
        """FQN for the Gold occupation-skill graph table.

        Example::

            sr.sr_gold.gold_occupation_skill_graph
        """
        return self.iceberg_table_fqn(
            "gold", "gold", "occupation_skill_graph", catalog=catalog, raw=False
        )
