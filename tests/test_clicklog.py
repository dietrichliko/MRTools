import logging

import mrtools.clicklog as clicklog

clicklog.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", datefmt="%y-%m-%d %H:%M:%S"
)
log = logging.getLogger()


def test_basic_config():

    assert isinstance(log.handlers[0], clicklog.ClickHandler)
