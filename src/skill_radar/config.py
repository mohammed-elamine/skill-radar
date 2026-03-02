import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    adzuna_app_id: str
    adzuna_app_key: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            adzuna_app_id=os.environ["ADZUNA_APP_ID"],
            adzuna_app_key=os.environ["ADZUNA_APP_KEY"],
        )


settings = Settings.from_env()
