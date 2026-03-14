"""Search serving domain — Gold → Elasticsearch export.

Reads analytics-ready Gold Iceberg tables and publishes them to
Elasticsearch for search and dashboards.  Gold/Iceberg remains the
source of truth; Elasticsearch is a serving layer only.
"""

from __future__ import annotations
