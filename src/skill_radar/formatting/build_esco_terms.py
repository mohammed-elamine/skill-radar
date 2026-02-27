from __future__ import annotations

from typing import Any

import pandas as pd

from skill_radar.config import settings


def _split_labels(x: Any) -> list[str]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return []
    s = str(x).strip()
    if not s or s.lower() in {"nan", "none"}:
        return []
    return [p.strip() for p in s.split("\n") if p.strip()]


def build_esco_terms() -> None:
    in_path = settings.data_root / "formatted" / "esco_skills.parquet"
    out_path = settings.data_root / "formatted" / "esco_skill_terms.parquet"

    df = pd.read_parquet(in_path)

    rows: list[tuple[str, str, str]] = []
    for _, r in df.iterrows():
        uri = r.get("concepturi")
        if not uri:
            continue

        pref = str(r.get("preferredlabel") or "").strip()
        if pref:
            rows.append((uri, pref, "preferred"))

        for t in _split_labels(r.get("altlabels")):
            rows.append((uri, t, "alt"))

        for t in _split_labels(r.get("hiddenlabels")):
            rows.append((uri, t, "hidden"))

    out = pd.DataFrame(rows, columns=["skilluri", "term", "term_type"])
    out["term_norm"] = out["term"].astype(str).str.lower().str.strip()
    out = out.drop_duplicates(subset=["skilluri", "term_norm", "term_type"])

    out.to_parquet(out_path, index=False)

    print(f"✅ Written: {out_path}")
    print(f"Rows: {len(out):,} | Unique skills: {out['skilluri'].nunique():,}")


def main() -> None:
    build_esco_terms()


if __name__ == "__main__":
    main()
