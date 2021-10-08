"""Modern ROOT Tools"""

import importlib.metadata
import logging
import pathlib
from typing import Union

import ROOT  # type: ignore

from mrtools.config import Configuration
from mrtools.exceptions import MRTError  # noqa: F401
from mrtools.samples_cache import SamplesCache  # noqa: F401
from mrtools.samples import (  # noqa: F401
    SampleABC,
    Sample,
    SampleGroup,
    SampleFromDAS,
    SampleFromFS,
)
from mrtools.commands import AnalyzerCli  # noqa: F401
from mrtools.analyzer import DFAnalyzer  # noqa: F401


PathOrStr = Union[pathlib.Path, str]

# add a null handler, in case logging is not prperly initialised
logging.getLogger(__package__).addHandler(logging.NullHandler())

__version__ = importlib.metadata.version(__package__)


def init(config_file: PathOrStr = "", site: str = "", root_threads: int = 0):

    config = Configuration()
    config.init(config_file, site)

    ROOT.gROOT.SetBatch()
    ROOT.PyConfig.IgnoreCommandLineOptions = True
    ROOT.EnableImplictMT(root_threads)

    ROOT.gSystem.Load(str(pathlib.Path(__file__).parent / "libMRTools.so"))
