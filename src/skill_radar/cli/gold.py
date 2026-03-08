"""CLI commands for Gold pipeline operations.

Provides thin entrypoints for:
- skill-radar gold matching  (run Gold matching stage)
- skill-radar gold analytics (run Gold analytics stage)
- skill-radar gold pipeline  (run full Gold pipeline)
"""

from __future__ import annotations

import logging
import sys

import click

from skill_radar.platform.logging import finalize_logging, init_logging, set_context

logger = logging.getLogger(__name__)

EXIT_SUCCESS = 0
EXIT_GOLD_ERROR = 9


@click.group("gold")
def gold_group() -> None:
    """Gold layer operations (matching / analytics)."""


# ---------------------------------------------------------------------------
# Gold matching
# ---------------------------------------------------------------------------


@gold_group.command()
@click.option("--ingestion-date", "ingestion_date", required=True, help="Adzuna date YYYY-MM-DD.")
@click.option("--country", required=True, help="Country code (e.g. fr).")
@click.option("--esco-version", "esco_version", required=True, help="ESCO version (e.g. v1.2.1).")
@click.option("--esco-lang", "esco_lang", required=True, help="ESCO language (e.g. fr).")
@click.option("--job-limit", "job_limit", default=None, type=int, help="Debug: limit Adzuna jobs.")
@click.option("--quiet", is_flag=True, default=False, help="Suppress normal output.")
def matching(ingestion_date, country, esco_version, esco_lang, job_limit, quiet):
    """Run Gold matching (job-skill + job-occupation)."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo("Error: PySpark is not installed. Gold matching requires Spark/Iceberg.")
        sys.exit(EXIT_GOLD_ERROR)

    from skill_radar.domains.gold.matching.orchestrator import run_gold_matching

    ctx = init_logging("gold_matching", enable_file=True)
    set_context(dataset="gold")

    spark = SparkSession.builder.appName("gold_matching").getOrCreate()
    set_context(spark_app_id=spark.sparkContext.applicationId)

    try:
        result = run_gold_matching(
            spark,
            ingestion_date=ingestion_date,
            country=country,
            esco_version=esco_version,
            esco_lang=esco_lang,
            job_limit=job_limit,
            run_id=ctx.run_id,
        )

        if result.success:
            if not quiet:
                click.echo("")
                click.echo("  Gold matching complete.")
                click.echo(f"    Country              : {result.country}")
                click.echo(f"    Ingestion date       : {result.ingestion_date}")
                click.echo(f"    ESCO version         : {result.esco_version}")
                click.echo(f"    ESCO lang            : {result.esco_lang}")
                click.echo(f"    Adzuna jobs          : {result.adzuna_jobs_count}")
                click.echo(f"    Skill dictionary     : {result.skill_dictionary_size}")
                click.echo(f"    Job-skill matches    : {result.job_skill_matches_count}")
                click.echo(f"    Job-occ matches      : {result.job_occupation_matches_count}")
                click.echo(f"    Run ID               : {result.run_id}")
                click.echo("")
            finalize_logging()
            sys.exit(EXIT_SUCCESS)

        click.echo("")
        click.echo(f"  Gold matching failed: {result.error}")
        finalize_logging()
        sys.exit(EXIT_GOLD_ERROR)

    except Exception as exc:
        logger.exception("Gold matching failed")
        click.echo(f"\n  Fatal error: {exc}")
        finalize_logging()
        sys.exit(EXIT_GOLD_ERROR)
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# Gold analytics
# ---------------------------------------------------------------------------


@gold_group.command()
@click.option("--ingestion-date", "ingestion_date", required=True, help="Adzuna date YYYY-MM-DD.")
@click.option("--country", required=True, help="Country code (e.g. fr).")
@click.option("--esco-version", "esco_version", required=True, help="ESCO version (e.g. v1.2.1).")
@click.option("--esco-lang", "esco_lang", required=True, help="ESCO language (e.g. fr).")
@click.option("--quiet", is_flag=True, default=False, help="Suppress normal output.")
def analytics(ingestion_date, country, esco_version, esco_lang, quiet):
    """Run Gold analytics (KPIs + occupation-skill graph)."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo("Error: PySpark is not installed. Gold analytics requires Spark/Iceberg.")
        sys.exit(EXIT_GOLD_ERROR)

    from skill_radar.domains.gold.analytics.orchestrator import run_gold_analytics

    ctx = init_logging("gold_analytics", enable_file=True)
    set_context(dataset="gold")

    spark = SparkSession.builder.appName("gold_analytics").getOrCreate()
    set_context(spark_app_id=spark.sparkContext.applicationId)

    try:
        result = run_gold_analytics(
            spark,
            ingestion_date=ingestion_date,
            country=country,
            esco_version=esco_version,
            esco_lang=esco_lang,
            run_id=ctx.run_id,
        )

        if result.success:
            if not quiet:
                click.echo("")
                click.echo("  Gold analytics complete.")
                click.echo(f"    Country              : {result.country}")
                click.echo(f"    Ingestion date       : {result.ingestion_date}")
                click.echo(f"    Skill demand rows    : {result.skill_demand_rows}")
                click.echo(f"    Salary-by-skill rows : {result.salary_by_skill_rows}")
                click.echo(f"    Occ-skill graph rows : {result.occupation_skill_graph_rows}")
                click.echo(f"    Emerging skills rows : {result.skill_emerging_rows}")
                click.echo(f"    Occupation market rows: {result.occupation_market_rows}")
                click.echo(f"    Skill segments rows  : {result.skill_demand_segments_rows}")
                click.echo(f"    Run ID               : {result.run_id}")
                click.echo("")
            finalize_logging()
            sys.exit(EXIT_SUCCESS)

        click.echo("")
        click.echo(f"  Gold analytics failed: {result.error}")
        finalize_logging()
        sys.exit(EXIT_GOLD_ERROR)

    except Exception as exc:
        logger.exception("Gold analytics failed")
        click.echo(f"\n  Fatal error: {exc}")
        finalize_logging()
        sys.exit(EXIT_GOLD_ERROR)
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# Gold pipeline (matching + analytics)
# ---------------------------------------------------------------------------


@gold_group.command()
@click.option("--ingestion-date", "ingestion_date", required=True, help="Adzuna date YYYY-MM-DD.")
@click.option("--country", required=True, help="Country code (e.g. fr).")
@click.option("--esco-version", "esco_version", required=True, help="ESCO version (e.g. v1.2.1).")
@click.option("--esco-lang", "esco_lang", required=True, help="ESCO language (e.g. fr).")
@click.option("--job-limit", "job_limit", default=None, type=int, help="Debug: limit Adzuna jobs.")
@click.option("--quiet", is_flag=True, default=False, help="Suppress normal output.")
def pipeline(ingestion_date, country, esco_version, esco_lang, job_limit, quiet):
    """Run full Gold pipeline: matching → analytics."""
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        click.echo("Error: PySpark is not installed. Gold pipeline requires Spark/Iceberg.")
        sys.exit(EXIT_GOLD_ERROR)

    from skill_radar.domains.gold.analytics.orchestrator import run_gold_analytics
    from skill_radar.domains.gold.matching.orchestrator import run_gold_matching

    ctx = init_logging("gold_pipeline", enable_file=True)
    set_context(dataset="gold")

    spark = SparkSession.builder.appName("gold_pipeline").getOrCreate()
    set_context(spark_app_id=spark.sparkContext.applicationId)

    try:
        # Phase 1: Matching
        if not quiet:
            click.echo("\n  ── Phase 1: Gold Matching ──\n")

        match_result = run_gold_matching(
            spark,
            ingestion_date=ingestion_date,
            country=country,
            esco_version=esco_version,
            esco_lang=esco_lang,
            job_limit=job_limit,
            run_id=ctx.run_id,
        )

        if not match_result.success:
            click.echo(f"\n  Gold matching failed: {match_result.error}")
            finalize_logging()
            sys.exit(EXIT_GOLD_ERROR)

        if not quiet:
            click.echo(f"    Job-skill matches    : {match_result.job_skill_matches_count}")
            click.echo(f"    Job-occ matches      : {match_result.job_occupation_matches_count}")

        # Phase 2: Analytics
        if not quiet:
            click.echo("\n  ── Phase 2: Gold Analytics ──\n")

        analytics_result = run_gold_analytics(
            spark,
            ingestion_date=ingestion_date,
            country=country,
            esco_version=esco_version,
            esco_lang=esco_lang,
            run_id=ctx.run_id,
        )

        if not analytics_result.success:
            click.echo(f"\n  Gold analytics failed: {analytics_result.error}")
            finalize_logging()
            sys.exit(EXIT_GOLD_ERROR)

        if not quiet:
            click.echo(f"    Skill demand rows    : {analytics_result.skill_demand_rows}")
            click.echo(f"    Salary-by-skill rows : {analytics_result.salary_by_skill_rows}")
            click.echo(f"    Occ-skill graph rows : {analytics_result.occupation_skill_graph_rows}")
            click.echo("")
            click.echo("  Gold pipeline complete.")
            click.echo(f"    Run ID               : {ctx.run_id}")
            click.echo("")

        finalize_logging()
        sys.exit(EXIT_SUCCESS)

    except Exception as exc:
        logger.exception("Gold pipeline failed")
        click.echo(f"\n  Fatal error: {exc}")
        finalize_logging()
        sys.exit(EXIT_GOLD_ERROR)
    finally:
        spark.stop()
