import pytest
import tomli

import ROOT  # type: ignore

from mrtools import __version__


def test_version() -> None:
    try:
        with open("pyproject.toml", "rb") as f:
            pyproject = tomli.load(f)
    except tomli.TOMLDecodeError as e:
        pytest.fail(f"Error parsing pyproject.toml: {e}", pytrace=False)
    assert __version__ == pyproject["tool"]["poetry"]["version"]


def test_library_load():

    assert ROOT.hello_world() == "Hello World"
