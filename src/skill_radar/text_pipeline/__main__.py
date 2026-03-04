from __future__ import annotations

import argparse
from pathlib import Path

from skill_radar.config import Settings
from skill_radar.text_pipeline.contracts import TextPipelinePaths
from skill_radar.text_pipeline.run import run


def main() -> None:
    parser = argparse.ArgumentParser(description="Run skill radar text pipeline")
    group = parser.add_mutually_exclusive_group()

    group.add_argument("--data-root", type=str, default=None)
    group.add_argument("--paths", action="store_true")

    parser.add_argument("--jobs-input", type=str, default=None)
    parser.add_argument("--esco-input", type=str, default=None)
    parser.add_argument("--matches-output", type=str, default=None)
    parser.add_argument("--kpis-output", type=str, default=None)

    args = parser.parse_args()

    settings = Settings.from_env()
    if args.data_root:
        settings = settings.with_data_root(args.data_root)

    # defaults (prod)
    jobs_input = args.jobs_input or str(settings.formatted_jobs_path)
    esco_input = args.esco_input or str(settings.esco_terms_path)
    matches_output = args.matches_output or str(settings.matches_output_root)
    kpis_output = args.kpis_output or str(settings.kpis_output_root)

    paths = TextPipelinePaths(
        jobs_input=Path(jobs_input).resolve(),
        esco_input=Path(esco_input).resolve(),
        matches_output=Path(matches_output).resolve(),
        kpis_output=Path(kpis_output).resolve(),
    )

    run(paths)


if __name__ == "__main__":
    main()
