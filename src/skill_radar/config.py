import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    adzuna_app_id: str
    adzuna_app_key: str
    data_root: Path  # NEW

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            adzuna_app_id=os.environ["ADZUNA_APP_ID"],
            adzuna_app_key=os.environ["ADZUNA_APP_KEY"],
            data_root=Path(os.getenv("DATA_ROOT", "./data")).resolve(),
        )


settings = Settings.from_env()
