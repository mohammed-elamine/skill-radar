import pytest

from skill_radar.utils.assertions import require


def test_require_ok():
    require(True, "should not fail")


def test_require_raises():
    with pytest.raises(ValueError):
        require(False, "boom")
