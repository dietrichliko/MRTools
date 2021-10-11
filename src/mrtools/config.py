"""Configuration for Modern ROOT Tools

Configuration is a singleton. As it it constructed only one, calling the constructur again
will connect to the same instance.

Usage:
    from mrtools.config import Configuration

    config = Configuration()
"""
import logging
import os
import pathlib
import shutil
import socket

import tomli
from typing import Optional, Dict, Union, Any, cast

from expandvars import expandvars
from datasize import DataSize

from mrtools.singleton import SingletonMetaClass
from mrtools.exceptions import MRTError

PathOrStr = Union[pathlib.Path, str]


log = logging.getLogger(__package__)

DEFAULTS_SITE = {
    "CLIP": {
        "store_path": "/eos/vbc/experiments/cms",
        "local_prefix": "root://eos.grid.vbc.ac.at/",
        "remote_prefix": "root://xrootd-cms.infn.it/",
        "stage": True,
        "file_cache_path": "/scratch-cbe/users/${USER}/file_cache",
    },
    "CERN": {
        "store_path": "/eos/cms",
        "local_prefix": "root://eoscms.cern.ch/",
        "remote_prefix": "root://xrootd-cms.infn.it/",
        "stage": False,
        "file_cache_path": "/afs/cern.ch/work/${USER:0:1}/${USER}/file_cache",
    },
    "Other": {
        "store_path": "",
        "local_prefix": "",
        "remote_prefix": "",
        "stage": False,
        "file_cache_path": "",
    },
}


class Configuration(metaclass=SingletonMetaClass):
    """Configuration wrapper class

    Usage:
        config = Configuration()
        config.init()
        print(config.bin.xrdcp)
    """

    bin: "Configuration.Binaries"
    site: "Configuration.Site"
    sc: "Configuration.SamplesCache"

    class Binaries:
        """Location of binary commands"""

        dasgoclient: str
        curl: str
        voms_proxy_info: str
        voms_proxy_init: str
        xrdcp: str

        def __init__(self, config_data: Dict[str, Any]) -> None:

            for name in [
                "dasgoclient",
                "curl",
                "voms-proxy-info",
                "voms-proxy-init",
                "xrdcp",
            ]:
                var_name = name.replace("-", "_")
                try:
                    path = config_data[var_name]
                except KeyError:
                    if (path := shutil.which(name)) is None:
                        raise MRTError(f"No binary found for {name}")
                if not os.access(path, os.X_OK):
                    raise MRTError(f"Binary {path} not found or not executable")
                setattr(self, var_name, path)

    class Site:
        """Site specific configuration"""

        name: str
        store_path: str
        local_prefix: str
        remote_prefix: str
        file_cache_path: pathlib.Path
        stage: bool

        def __init__(self, config_data: Dict[str, Any], site: str = "") -> None:

            if not site:
                domain_to_site = {"cbe.vbc.ac.at": "CLIP", "cern.ch": "CERN"}
                for site, config in config_data.items():
                    if "domains" in config:
                        domain_to_site |= {domain: site for domain in config["domains"]}

                domain = domainname()
                try:
                    site = domain_to_site[domain]
                    log.debug('Site "%s" for domain %s', site, domain)
                except KeyError:
                    raise MRTError(f"Could not determine site for {domain}")

            def_site = site if site in ["CLIP", "CERN"] else "Other"
            site_data = DEFAULTS_SITE[def_site]
            if site in config_data:
                site_data |= config_data[site]

            self.store_path = cast(str, site_data["store_path"])
            self.local_prefix = cast(str, site_data["local_prefix"])
            self.remote_prefix = cast(str, site_data["remote_prefix"])
            self.file_cache_path = expandpath(cast(str, site_data["file_cache_path"]))
            self.stage = cast(bool, site_data["stage"])
            self.name = site

    class SamplesCache:

        voms_proxy_path: pathlib.Path
        threads: int
        root_threads: int
        root_cache_size: DataSize
        xrdcp_retry: int
        db_path: pathlib.Path
        db_sql_echo: bool
        lockfile: bool
        lockfile_max_count: int
        lockfile_max_age: int

        def __init__(self, config_data: Dict[str, Any]) -> None:

            self.voms_proxy_path = expandpath(
                cast(str, config_data.get("voms_proxy_path", "~/private/.proxy"))
            )
            self.threads = cast(int, config_data.get("threads", 4))
            self.root_threads = cast(int, config_data.get("root_threads", 0))
            self.root_cache_size = DataSize(config_data.get("root_cache_size", 0))
            self.xrdcp_retry = cast(int, config_data.get("xrdcp_retry", 3))
            default_db_path = os.path.join(
                os.environ.get("XDG_CACHE_HOME", "~/.cache"), "mrtools/sample.db"
            )
            self.db_path = expandpath(
                cast(str, config_data.get("db_path", default_db_path))
            )
            self.db_sql_echo = config_data.get("db_sql_echo", 0)
            self.lockfile = config_data.get("lockfile", True)
            self.lockfile_max_count = config_data.get("lockfile_max_count", 6)
            self.lockfile_max_age = config_data.get("lockfile_max_age", 300)

    def init(self, config_file: Optional[PathOrStr] = None, site: str = "") -> None:
        """Initialise configuration

        The configuration file is stored in ~/.config/mrtools/mrtools.toml or
        at $XDG_CONFIG_HOME/mrtools/mrtools.toml
        Arguments:
            config_file (PathOrStr): other file to be read
            site (str): force the configuration for a site
        """

        if config_file is None:
            config_file = (
                pathlib.Path(
                    os.environ.get("XDG_CONFIG_HOME", "~/.config")
                ).expanduser()
                / "mrtools/mrtools.toml"
            )

        config_path = (
            pathlib.Path(config_file) if isinstance(config_file, str) else config_file
        )
        if not config_path.exists():
            log.info("Creating configuration file %s", config_file)
            config_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            shutil.copyfile(
                pathlib.Path(__file__).with_name("mrtools.toml"), config_path
            )

        with open(config_file, "rb") as f:
            config_data = tomli.load(f)

        self.bin = Configuration.Binaries(config_data.get("binaries", {}))
        self.site = Configuration.Site(config_data.get("site", {}), site)
        self.sc = Configuration.SamplesCache(config_data.get("samples_cache", {}))


def domainname() -> str:
    """Simply the domainname"""

    return socket.getfqdn().split(".", 1)[1]


def expandpath(name: str) -> pathlib.Path:
    """Expand a path with environment variable and tilde expansion

    The library expandvars provides for many bash features.
    """

    return pathlib.Path(expandvars(name)).expanduser()
