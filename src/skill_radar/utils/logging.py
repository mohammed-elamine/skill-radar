"""Backward-compatible logging shim.

.. deprecated::
    Use ``logging.getLogger(__name__)`` in modules and call
    :func:`skill_radar.platform.logging.init_logging` once at process entry.
"""

import logging
import warnings


def setup_logging(name: str) -> logging.Logger:
    """Return a logger for *name*.

    .. deprecated::
        Retained for backward compatibility only.  New code should use
        ``logging.getLogger(__name__)``.
    """
    warnings.warn(
        "setup_logging() is deprecated — use logging.getLogger(__name__) "
        "and call init_logging() once at your entry-point.",
        DeprecationWarning,
        stacklevel=2,
    )
    return logging.getLogger(name)


def get_logger(name: str) -> logging.Logger:
    """Thin wrapper around :func:`logging.getLogger`."""
    return logging.getLogger(name)
