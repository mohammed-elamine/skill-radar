"""Configuration package for the Skill Radar platform.

Public API
----------
- :func:`load_platform_config` — load validated platform settings.
- :class:`PlatformSettings` — Pydantic model for platform config.
"""

from __future__ import annotations

from skill_radar.config.loader import load_platform_config
from skill_radar.config.models import PlatformSettings

__all__ = [
    "PlatformSettings",
    "load_platform_config",
]
