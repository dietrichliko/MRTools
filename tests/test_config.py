import pathlib
import tempfile
import os

import pytest
from pytest_mock import MockerFixture

import mrtools

config = mrtools.Configuration()


def test_userfile_creation(mocker: MockerFixture):

    with tempfile.TemporaryDirectory() as t:
        mocker.patch("os.environ.get", return_value=t)
        mocker.patch("shutil.which", return_value="/bin/false")
        config.init(site="clip")
        assert os.access(os.path.join(t, "mrtools/mrtools.toml"), os.R_OK)


def test_binaries(mocker: MockerFixture) -> None:

    mocker.patch("shutil.which", return_value="/bin/false")
    config.init(pathlib.Path(mrtools.__file__).with_name("mrtools.toml"), site="clip")

    assert config.bin.dasgoclient == "/bin/false"
    assert config.bin.curl == "/bin/false"
    assert config.bin.voms_proxy_info == "/bin/false"
    assert config.bin.voms_proxy_init == "/bin/false"
    assert config.bin.xrdcp == "/bin/false"


def test_binaries_not_found(mocker: MockerFixture) -> None:

    mocker.patch("shutil.which", return_value=None)

    with pytest.raises(mrtools.MRTError):
        config.init(pathlib.Path(mrtools.__file__).with_name("mrtools.toml"))


def test_binaries_not_executable(mocker: MockerFixture) -> None:

    mocker.patch("shutil.which", return_value="/bin/none")

    with pytest.raises(mrtools.MRTError):
        config.init(pathlib.Path(mrtools.__file__).with_name("mrtools.toml"))


def test_site_clip(mocker: MockerFixture) -> None:

    mocker.patch("socket.getfqdn", return_value="clip-login-1.cbe.vbc.ac.at")
    config.init(pathlib.Path(mrtools.__file__).with_name("mrtools.toml"))

    assert config.site.name == "clip"
    assert config.site.store_path == "/eos/vbc/experiments/cms"
    assert config.site.local_prefix == "root://eos.grid.vbc.ac.at/"
    assert config.site.remote_prefix == "root://xrootd-cms.infn.it/"
    assert config.site.stage == True
    assert config.site.file_cache_path == pathlib.Path(
        f"/scratch-cbe/users/{os.environ['USER']}/file_cache"
    )


def test_site_cern(mocker: MockerFixture) -> None:

    mocker.patch("socket.getfqdn", return_value="lxplus.cern.ch")
    config.init(pathlib.Path(mrtools.__file__).with_name("mrtools.toml"))

    assert config.site.name == "cern"
    assert config.site.store_path == "/eos/cms"
    assert config.site.local_prefix == "root://eoscms.cern.ch/"
    assert config.site.remote_prefix == "root://xrootd-cms.infn.it/"
    assert config.site.stage == False
    user = os.environ['USER']
    assert config.site.file_cache_path == pathlib.Path(
        f"/afs/cern.ch/work/{user[0]}/{user}/file_cache"
    )


def test_site_not_found(mocker: MockerFixture) -> None:

    mocker.patch("socket.getfqdn", return_value="dummy.dummy.net")
    with pytest.raises(mrtools.MRTError):
        config.init(pathlib.Path(mrtools.__file__).with_name("mrtools.toml"))
