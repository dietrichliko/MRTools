import os
import pathlib

import pytest
from pytest_mock import MockerFixture

import mrtools
from mrtools.utils import domainname, expandpath, xrd_checksum


def test_domainname(mocker: MockerFixture) -> None:

    mocker.patch("socket.getfqdn", return_value="test.domain.com")

    assert domainname() == "domain.com"


def test_expandpath():

    assert expandpath("/test/$USER") == pathlib.Path(f"/test/{os.environ['USER']}")
    assert expandpath("~/") == pathlib.Path(os.environ["HOME"])


def test_xrd_checksum():

    path = pathlib.Path(__file__).with_name("test_checksum.dat")
    assert xrd_checksum(str(path)) == 3348234566


def test_xrd_checksum_not_exists():

    path = pathlib.Path(__file__).with_name("not_exist.dat")
    with pytest.raises(mrtools.MRTError):
        xrd_checksum(str(path))
