"""Path validation rules for lake layout conventions."""

from __future__ import annotations

import re

# Pattern: root/layer/domain/source/entity[/key=value]*
_LAKE_PATH_RE = re.compile(r"^[a-z][a-z0-9_-]*/[a-z]+/[a-z]+/[a-z]+/[a-z_]+(/[a-z_]+=[^/]+)*$")


def is_valid_lake_path(path: str) -> bool:
    """Return ``True`` if *path* conforms to lake naming conventions."""
    return bool(_LAKE_PATH_RE.match(path))
