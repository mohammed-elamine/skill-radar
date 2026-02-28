import argparse
from pathlib import Path

from skill_radar.config import Settings
from skill_radar.text_pipeline.contracts import TextPipelinePaths
from skill_radar.text_pipeline.run import run


def build_paths(
    *,
    settings: Settings = Settings.from_env(),
    jobs_input: str | None = None,
    esco_input: str | None = None,
    matches_output: str | None = None,
    kpis_output: str | None = None,
) -> TextPipelinePaths:
    # Defaults come from DATA_ROOT conventions
    default_jobs = settings.formatted_jobs_partitioned_root
    default_esco = settings.esco_root
    default_matches = settings.curated_text_root / "job_skill_matches"
    default_kpis = settings.curated_kpis_root / "skill_kpis_daily"

    return TextPipelinePaths(
        jobs_input=Path(jobs_input).resolve() if jobs_input else default_jobs,
        esco_input=Path(esco_input).resolve() if esco_input else default_esco,
        matches_output=Path(matches_output).resolve() if matches_output else default_matches,
        kpis_output=Path(kpis_output).resolve() if kpis_output else default_kpis,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Run the skill radar text pipeline: extract skills, compute KPIs, save results."
    )
    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        "--data-root",
        type=str,
        default=None,
        help="Root of data lake. Paths are derived by convention.",
    )

    group.add_argument(
        "--paths",
        action="store_true",
        help="Use explicit input/output paths (provide the --jobs-input/--esco-input/... flags).",
    )

    # Explicit path flags (only meaningful when --paths is used)
    parser.add_argument("--jobs-input", type=str, default=None)
    parser.add_argument("--esco-input", type=str, default=None)
    parser.add_argument("--matches-output", type=str, default=None)
    parser.add_argument("--kpis-output", type=str, default=None)

    args = parser.parse_args()

    settings = Settings.from_env()
    if args.data_root:
        settings = settings.with_data_root(args.data_root)

        paths = build_paths(settings=settings)
    else:
        paths = build_paths(
            settings=settings,
            jobs_input=args.jobs_input,
            esco_input=args.esco_input,
            matches_output=args.matches_output,
            kpis_output=args.kpis_output,
        )

    run(paths)


if __name__ == "__main__":
    main()
