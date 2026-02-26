from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

import requests

from skill_radar.config import settings

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class AdzunaIngestConfig:
    country: str = "fr"
    query: str = "data"
    results_per_page: int = 50
    max_pages: int = 3
    sleep_between: float = 1.0


def fetch_page(cfg: AdzunaIngestConfig, page: int) -> dict[str, Any]:
    url = f"https://api.adzuna.com/v1/api/jobs/{cfg.country}/search/{page}"
    params = {
        "app_id": settings.adzuna_app_id,
        "app_key": settings.adzuna_app_key,
        "results_per_page": cfg.results_per_page,
        "what": cfg.query,
        "content-type": "application/json",
    }
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()


def ingest_adzuna_jobs(cfg: AdzunaIngestConfig, data_root: Path) -> Path:
    """Ingest Adzuna jobs and write raw JSON pages + meta.json. Returns output dir."""
    today = datetime.now().strftime("%Y-%m-%d")
    out_dir = data_root / "raw" / "jobs" / today
    out_dir.mkdir(parents=True, exist_ok=True)

    meta: dict[str, Any] = {
        "date": today,
        "country": cfg.country,
        "query": cfg.query,
        "results_per_page": cfg.results_per_page,
        "max_pages": cfg.max_pages,
        "pages_fetched": 0,
        "total_results_in_files": 0,
        "saved_files": [],
        "status": "started",
        "started_at": datetime.now(datetime.UTC).isoformat(),
    }

    print(
        f"📥 Fetching Adzuna jobs: country={cfg.country}, query='{cfg.query}', pages={cfg.max_pages}"
    )

    try:
        for page in range(1, cfg.max_pages + 1):
            data = fetch_page(cfg, page)
            results = data.get("results", [])

            file_path = out_dir / f"jobs_raw_page_{page}.json"
            file_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

            meta["pages_fetched"] += 1
            meta["total_results_in_files"] += len(results)
            meta["saved_files"].append(str(file_path))

            print(f"✅ Saved page {page}: {len(results)} results -> {file_path}")
            time.sleep(cfg.sleep_between)

        meta["status"] = "success"

    except Exception as e:
        meta["status"] = "failed"
        meta["error"] = str(e)
        print(f"❌ Ingestion failed: {e}")

    finally:
        meta["finished_at"] = datetime.now(datetime.UTC).isoformat()
        meta_path = out_dir / "meta.json"
        meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
        print(f"🧾 Meta written: {meta_path}")

    return out_dir


def main() -> None:
    cfg = AdzunaIngestConfig()
    ingest_adzuna_jobs(cfg, settings.data_root)


if __name__ == "__main__":
    main()
