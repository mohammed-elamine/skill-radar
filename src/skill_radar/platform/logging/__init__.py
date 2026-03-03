"""Production-aligned logging package for the Skill Radar platform.

Public API
----------
.. autofunction:: init_logging
.. autofunction:: finalize_logging
.. autofunction:: set_context
.. autofunction:: get_context
"""

from skill_radar.platform.logging.bootstrap import finalize_logging, init_logging
from skill_radar.platform.logging.context import get_context, set_context

__all__ = [
    "finalize_logging",
    "get_context",
    "init_logging",
    "set_context",
]
