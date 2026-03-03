"""Configuration package for the Skill Radar platform.

Re-exports for backward compatibility and convenience.
"""

from __future__ import annotations

# Backward-compatible re-export of the Adzuna settings.
from skill_radar.config.adzuna import Settings, get_settings
from skill_radar.config.loader import load_platform_config
from skill_radar.config.models import PlatformSettings

__all__ = [
    "PlatformSettings",
    "Settings",
    "get_settings",
    "load_platform_config",
]
