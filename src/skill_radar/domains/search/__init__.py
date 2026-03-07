"""Search serving domain — Gold → Elasticsearch export.

This package implements the serving/export stage that reads analytics-ready
Gold Iceberg tables and publishes them to Elasticsearch for search and
dashboards.

Gold/Iceberg remains the **source of truth**.  Elasticsearch is a
serving/indexing layer only — no business logic or KPI recomputation
happens here.

Modules
-------
datasets
    Registry of served datasets with source table FQNs and mappings.
documents
    Pure functions converting Spark rows to Elasticsearch documents.
export
    Orchestrator: read Gold → transform → bulk index → alias swap.
"""

from __future__ import annotations
