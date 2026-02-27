from __future__ import annotations

import pandas as pd

from skill_radar.config import settings


def _read_csv(raw_dir, name: str) -> pd.DataFrame:
    path = raw_dir / name
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}")
    df = pd.read_csv(path, dtype=str, low_memory=False)
    df.columns = [c.strip().lower() for c in df.columns]
    return df


def _clean_str(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.columns:
        s = df[c].astype("string").str.strip()
        s = s.replace({"": None, "nan": None, "None": None})
        df[c] = s
    return df


def format_esco() -> None:
    raw_dir = settings.data_root / "raw" / "esco"
    out_dir = settings.data_root / "formatted"
    out_dir.mkdir(parents=True, exist_ok=True)

    files = {
        "skills_fr.csv": "esco_skills.parquet",
        "dictionary_fr.csv": "esco_skill_labels.parquet",
        "skillsHierarchy_fr.csv": "esco_skills_hierarchy.parquet",
        "skillGroups_fr.csv": "esco_skill_groups.parquet",
        "occupations_fr.csv": "esco_occupations.parquet",
        "occupationSkillRelations_fr.csv": "esco_occ_skill_rel.parquet",
    }

    for in_name, out_name in files.items():
        df = _clean_str(_read_csv(raw_dir, in_name))
        df.to_parquet(out_dir / out_name, index=False)
        print(f"✅ {out_name}")

    print("\n🎉 ESCO formatted layer created in data/formatted/")


def main() -> None:
    format_esco()


if __name__ == "__main__":
    main()
