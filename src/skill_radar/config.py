import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    # Secrets
    adzuna_app_id: str
    adzuna_app_key: str

    # Paths
    data_root: Path

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            adzuna_app_id=os.environ["ADZUNA_APP_ID"],
            adzuna_app_key=os.environ["ADZUNA_APP_KEY"],
            data_root=Path(os.environ.get("DATA_ROOT", "data")).resolve(),
        )

    def with_data_root(self, new_root: str | Path) -> "Settings":
        return Settings(
            adzuna_app_id=self.adzuna_app_id,
            adzuna_app_key=self.adzuna_app_key,
            data_root=Path(new_root).resolve(),
        )

    # ---- Structured path builders ----
    @property
    def curated_root(self) -> Path:
        return self.data_root / "curated"

    @property
    def formatted_root(self) -> Path:
        return self.data_root / "formatted"

    @property
    def formatted_jobs_root(self) -> Path:
        return self.formatted_root / "jobs"

    @property
    def formatted_jobs_partitioned_root(self) -> Path:
        return self.formatted_jobs_root / "partitioned"

    @property
    def formatted_jobs_all_root(self) -> Path:
        return self.formatted_jobs_root / "jobs_all"

    @property
    def esco_root(self) -> Path:
        return self.curated_root / "reference" / "esco_skills"

    @property
    def curated_text_root(self) -> Path:
        return self.curated_root / "text"

    @property
    def curated_kpis_root(self) -> Path:
        return self.curated_root / "kpis"


settings = Settings.from_env()
