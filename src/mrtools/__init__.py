"""Modern ROOT Tools"""

import importlib.metadata
import logging
import pathlib
from typing import Union

from mrtools.config import Configuration  # noqa: F401
from mrtools.analyzer import DFAnalyzer  # noqa: F401
from mrtools.samples_cache import SamplesCache  # noqa: F401
from mrtools.samples import SampleABC  # noqa: F401
from mrtools.commands import AnalyzerCli  # noqa: F401


PathOrStr = Union[pathlib.Path, str]

# add a null handler, in case logging is not prperly initialised
logging.getLogger(__package__).addHandler(logging.NullHandler())

__version__ = importlib.metadata.version(__package__)
