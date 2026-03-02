from skill_radar.utils.logging import setup_logging


def test_setup_logging():
    logger = setup_logging("x")
    assert logger.name == "x"
