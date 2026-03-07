"""Search serving platform agent — Elasticsearch + Kibana integration.

This package provides the low-level infrastructure layer for reading from
Gold Iceberg tables and writing to Elasticsearch indices.  It contains no
business logic; that lives in :mod:`skill_radar.domains.search`.

Modules
-------
naming
    Single source of truth for index names and aliases.
client
    Thin Elasticsearch client wrapper (connectivity, index lifecycle).
bulk
    Bulk indexing with chunking, retries, and structured failure reporting.
mappings
    Declarative Elasticsearch mappings and settings for served datasets.
models
    Data models for search export results and metadata.
kibana
    Kibana data view and saved object bootstrap helpers.
"""

from __future__ import annotations
