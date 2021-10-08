"""Various utility functions"""

import socket
import subprocess
import logging
import pathlib
import zlib
from typing import Union

from XRootD import client as xrd_client  # type: ignore
from XRootD.client.flags import OpenFlags as xrd_OpenFlags  # type: ignore
from expandvars import expandvars

from mrtools.exceptions import MRTError

PathOrStr = Union[pathlib.Path, str]

log = logging.getLogger(__package__)

def xrd_checksum(url: str) -> int:
    """Calculate adler32 checksum of file reading its content with XRootD

    Usage:
        checksum = xrd_checksum("root://eos.grid.vbc.at.at//eos/.vbc/experiments/cms/store/...")
    """

    checksum: int = 1
    with xrd_client.File() as f:
        status = f.open(url, xrd_OpenFlags.READ)
        if not status[0].ok:
            raise MRTError(status[0].message)
        checksum = 1
        for chunk in f.readchunks():
            checksum = zlib.adler32(chunk, checksum)

    return checksum



