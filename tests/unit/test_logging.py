"""Tests for the legacy logging shim (backward compatibility)."""

import warnings

from skill_radar.utils.logging import get_logger, setup_logging


def test_setup_logging_returns_logger():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        logger = setup_logging("x")
    assert logger.name == "x"


def test_setup_logging_emits_deprecation_warning():
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        setup_logging("y")
    assert any(issubclass(x.category, DeprecationWarning) for x in w)


def test_get_logger():
    logger = get_logger("z")
    assert logger.name == "z"
