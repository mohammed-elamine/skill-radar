"""Gold business layer — cross-source analytics and matching.

This domain combines ESCO Silver taxonomy/reference data with Adzuna Silver
daily job facts to produce analytics-ready Gold Iceberg tables:

- ``gold_job_skill_matches``: bridge table between jobs and matched ESCO skills
- ``gold_job_occupation_matches``: bridge table between jobs and inferred occupations
- ``gold_skill_demand_daily``: daily demand KPIs by skill
- ``gold_salary_by_skill_daily``: daily salary metrics by skill
- ``gold_occupation_skill_graph``: occupation ↔ skill relationship enriched with market data
"""
