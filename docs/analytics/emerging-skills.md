# Emerging Skills

Detecting skill trends via momentum, acceleration, novelty, and composite scoring.

## Purpose

Identify skills that are growing in demand, accelerating, or newly appearing in the job market. Produces the `skill_emerging_daily` Gold table (Phase 6).

## Scoring Methodology

### Momentum Score

Measures short-term growth: ratio of recent demand to historical baseline.

```
momentum = recent_demand / baseline_demand
```

Where `recent_demand` is the average daily job count over the last N days and `baseline_demand` is the average over the full observation window.

### Acceleration Score

Measures the rate of change of momentum — is the growth itself accelerating?

```
acceleration = (momentum_current - momentum_previous) / momentum_previous
```

Computed over sliding windows to detect inflection points.

### Novelty Score

Measures recency of first appearance. Skills that appeared very recently get higher novelty scores.

```
novelty = 1 - (days_since_first_appearance / observation_window)
```

Clamped to [0, 1]. A brand-new skill scores 1.0; one that has been present for the full window scores 0.0.

### Composite Score

Weighted combination of all three dimensions:

```
composite = w_momentum × momentum + w_acceleration × acceleration + w_novelty × novelty
```

Default weights: `w_momentum = 0.4`, `w_acceleration = 0.3`, `w_novelty = 0.3`.

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `esco_skill_concept_uri` | string | ESCO skill URI |
| `esco_skill_preferred_label` | string | Skill name |
| `momentum_score` | double | Short-term growth rate |
| `acceleration_score` | double | Rate of momentum change |
| `novelty_score` | double | Recency of first appearance |
| `composite_score` | double | Weighted combination |
| `recent_jobs_count` | long | Recent observation window demand |
| `baseline_jobs_count` | long | Full-window baseline demand |
| `first_seen_date` | string | Date of first appearance |
| `ingestion_date`, `country` | string | Partition keys |
| `gold_pipeline_version`, `gold_computed_at` | string | Lineage |

**Key:** `(esco_skill_concept_uri, country, ingestion_date)`

## Soft-Fail Behavior

This phase runs with `try/except` — if it fails (e.g., insufficient historical data), the error is logged and subsequent phases continue unaffected.

## Module

`src/skill_radar/domains/gold/analytics/emerging_skills.py`

## References

- [Gold Pipeline](../pipelines/gold.md) — Phase 6 context
- [ML Features](ml-features.md) — demand segmentation
- [Data Model](../architecture/data-model.md) — full schema reference
