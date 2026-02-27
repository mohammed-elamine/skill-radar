from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _resolve_from_repo_root(p: str, repo_root: Path) -> Path:
    path = Path(p)
    return path if path.is_absolute() else (repo_root / path).resolve()


@dataclass(frozen=True)
class Settings:
    adzuna_app_id: str
    adzuna_app_key: str
    data_root: Path
    duckdb_path: Path

    @classmethod
    def from_env(cls) -> Settings:
        # repo root = .../skill-radar
        repo_root = Path(__file__).resolve().parents[2]

        data_root_env = os.getenv("DATA_ROOT", "data")
        duckdb_env = os.getenv("DUCKDB_PATH", "data/dev.duckdb")

        data_root = _resolve_from_repo_root(data_root_env, repo_root)
        duckdb_path = _resolve_from_repo_root(duckdb_env, repo_root)

        return cls(
            adzuna_app_id=os.environ["ADZUNA_APP_ID"],
            adzuna_app_key=os.environ["ADZUNA_APP_KEY"],
            data_root=data_root,
            duckdb_path=duckdb_path,
        )


settings = Settings.from_env()
