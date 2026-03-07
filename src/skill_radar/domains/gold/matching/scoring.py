"""Centralized match-score assignment — single source of truth for scoring.

All score literals live in :mod:`~.models` (``SKILL_MATCH_SCORES``,
occupation scoring constants).  This module builds **Spark Column
expressions** from those constants so that:

- Score assignment is not duplicated across matching modules.
- Matching modules and validation remain aligned.
- Future tuning only requires changing ``models.py``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from .models import SKILL_MATCH_SCORES

if TYPE_CHECKING:
    from pyspark.sql.column import Column


def build_skill_score_expr(
    label_type_col: str | Column,
    text_source_col: str | Column,
) -> Column:
    """Build a Spark Column for deterministic skill match scoring.

    Parameters
    ----------
    label_type_col:
        Column (or name) holding the label type (``preferred`` / ``alt``
        / ``hidden``).
    text_source_col:
        Column (or name) holding the text source (``title`` /
        ``description``).

    Returns
    -------
    Column
        A ``CASE … WHEN … THEN …`` expression that maps each
        (label_type, text_source) pair to the score defined in
        :data:`~.models.SKILL_MATCH_SCORES`.  Unknown combinations
        evaluate to ``0.0``.
    """
    _type = F.col(label_type_col) if isinstance(label_type_col, str) else label_type_col
    _src = F.col(text_source_col) if isinstance(text_source_col, str) else text_source_col

    expr: Column | None = None
    for (lt, ts), score in SKILL_MATCH_SCORES.items():
        cond = (_type == lt) & (_src == ts)
        expr = F.when(cond, F.lit(score)) if expr is None else expr.when(cond, F.lit(score))

    if expr is None:
        return F.lit(0.0)

    return expr.otherwise(F.lit(0.0))
