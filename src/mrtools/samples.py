"""Samples arr the Dataobject

   SampleABC
   - SampleGroup
   - Sample
    - SampleFromDAS
    - SampleFromFS

"""

import abc
import collections
import enum
import fnmatch
import itertools
import json
import logging
import operator
import os
import pathlib
import subprocess
import sys
from typing import Any, Dict, Iterable, Iterator, Optional, Union, cast

import ROOT  # type: ignore
from datasize import DataSize

import mrtools

log = logging.getLogger(__package__)
config = mrtools.Configuration()

# type alias
PathOrStr = Union[str, pathlib.Path]
PurePathOrStr = Union[str, pathlib.PurePath]


class FileFlags:
    """Various flags describing the state of a file"""

    class Status(enum.Enum):
        OK = 0
        BAD = 1

    class Location(enum.Enum):
        LOCAL = 0
        REMOTE = 1

    class StageStatus(enum.Enum):
        UNSTAGED = 0
        STAGING = 1
        STAGED = 2

    status: Status
    location: Location
    stage_status: StageStatus

    def __init__(
        self,
        *,
        status: Status = Status.OK,
        location: Location = Location.LOCAL,
        stage_status: StageStatus = StageStatus.UNSTAGED,
    ) -> None:

        self.status = status
        self.location = location
        self.stage_status = stage_status

    def __str_(self) -> str:

        return f"{self.status.name}, {self.location.name}, {self.stage_status.name}"


class File:
    """File as part of sample"""

    _sample: "Sample"
    _dirname: str
    _name: str
    _flags: FileFlags
    _entries: Optional[int]
    _size: Optional[DataSize]
    _checksum: Optional[int]

    def __init__(
        self,
        sample: "Sample",
        path: PathOrStr,
        *,
        status: FileFlags.Status = FileFlags.Status.OK,
        location: FileFlags.Location = FileFlags.Location.LOCAL,
        stage_status: FileFlags.StageStatus = FileFlags.StageStatus.UNSTAGED,
        entries: Optional[int] = None,
        size: Union[int, DataSize, None] = None,
        checksum: Optional[int] = None,
    ) -> None:

        self._sample = sample
        dirname, name = os.path.split(path)
        self._dirname = sys.intern(dirname)
        self._name = name
        self._flags = FileFlags(
            status=status,
            location=location,
            stage_status=stage_status,
        )
        self._entries = entries
        self._size = DataSize(size) if isinstance(size, int) else size
        self._checksum = checksum

    def __str__(self) -> str:
        return os.path.join(self._dirname, self._name)

    def __repr__(self) -> str:
        r = f'File(path="{self}", state={self._flags}'
        if self._entries is not None:
            r += f", entries={self._entries}"
        if self._size is not None:
            r += f", size={self.size:.2a}"
        if self._checksum is not None:
            r += f", checksum={self._checksum:08X}"

        return r + ")"

    @property
    def size(self) -> DataSize:
        """Size of file in bytes"""

        if self._size is None:
            raise mrtools.MRTError(f"File {self} has no size attribute.")

        return self._size

    @property
    def entries(self) -> int:
        """Number of entries of the ROOT chain"""

        if self._entries is None:
            raise mrtools.MRTError(f"File {self} has no entries attribute.")

        return self._entries

    @property
    def checksum(self) -> int:
        """Adler32 checksum of the file"""

        if self._checksum is None:
            raise mrtools.MRTError(f"File {self} has no checksum attribute.")

        return self._checksum

    @property
    def url_or_path(self) -> str:
        """return the file url"""

        path = str(self)
        if path.startswith("/store/") or path.startswith("/eos/"):
            if self._flags.stage_status == FileFlags.StageStatus.STAGED:
                return str(config.site.file_cache_path / path[1:])
            else:
                if self._flags.location == FileFlags.Location.LOCAL:
                    return config.site.local_prefix + path
                else:
                    return config.site.remote_prefix + path
        else:
            return path

    def get_entries(self) -> int:

        if not self._sample._tree_name:
            raise mrtools.MRTError("Tree name is undefined")

        f = ROOT.TFile(self.url_or_path, "READ")
        tree = f.Get(self._sample._tree_name)
        entries = cast(int, tree.GetEntries())  # fix mypy
        f.Close()

        log.debug("File %s has %d entries", str(self), entries)

        return entries

    def get_size(self) -> DataSize:

        size = DataSize(os.stat(str(self)).st_size)

        log.debug("File %s has %s", str(self), format(size, "a"))

        return size

    def get_checksum(self, from_eos: bool = True) -> int:

        if from_eos:
            try:
                checksum = int(
                    os.getxattr(str(self), "eos.checksum").decode("utf-8"), 16
                )
            except OSError as exc:
                raise mrtools.MRTError(
                    f"Could not retrieve the checksum metadata for {self}"
                ) from exc

        log.debug("File %s has checksum %x", str(self), checksum)

        return checksum


class SampleABC(collections.abc.Mapping):
    """Base class for Sample and SampleGroup"""

    _name: str
    _dirname: str
    _title: str

    def __init__(self, path: PurePathOrStr, *, title: Optional[str] = None) -> None:

        self._dirname, self._name = os.path.split(path)
        self._dirname = sys.intern(self._dirname)
        self._title = "" if title is None else title

    def __str__(self) -> str:

        return os.path.join(self._dirname, self._name)

    @property
    @abc.abstractmethod
    def samples(self) -> Dict[str, "Sample"]:
        pass

    @property
    def path(self) -> pathlib.PurePath:

        return pathlib.PurePath(self._dirname) / self._name

    @property
    def title(self) -> str:

        return self._title if self._title else self._name

    @property
    def entries(self) -> int:

        return sum(map(operator.attrgetter("entries"), iter(self)))

    @property
    def size(self) -> DataSize:

        return DataSize(sum(map(operator.attrgetter("size"), iter(self))))


class Sample(SampleABC):

    _tree_name: str
    _cross_section: Optional[float]
    _data: bool
    _files: Dict[str, Dict[str, File]]

    def __init__(
        self,
        path: PurePathOrStr,
        tree_name: str,
        *,
        title: Optional[str] = None,
        cross_section: Optional[float] = None,
        data: bool = False,
    ) -> None:

        super().__init__(path, title=title)

        self._tree_name = tree_name
        self._cross_section = cross_section
        self._data = data
        self._files = collections.defaultdict(dict)

    def __repr__(self) -> str:

        r = f'Sample(path="{self}"'
        if self._title is not None:
            r += f', title="{self._title}"'
        r += (
            f', tree_name="{self._tree_name}", #files={len(self)}, size={self.size:.2a}'
        )
        if self._cross_section is not None:
            r += f", cross_section={self._cross_section}"
        return r + f", data={self._data})"

    def __getitem__(self, obj: object) -> File:

        try:
            dirname, name = os.path.split(obj)  # type: ignore
            return self._files[dirname][name]
        except (KeyError, TypeError):
            raise KeyError(obj)

    def __iter__(self) -> Iterator[File]:

        return itertools.chain.from_iterable(f.values() for f in self._files.values())

    def __len__(self) -> int:

        return sum(len(f) for f in self._files.values())

    @property
    def samples(self) -> Dict[str, "Sample"]:
        return {str(self): self}

    def put_file(
        self,
        path: PathOrStr,
        *,
        status: FileFlags.Status = FileFlags.Status.OK,
        location: FileFlags.Location = FileFlags.Location.LOCAL,
        stage_status: FileFlags.StageStatus = FileFlags.StageStatus.UNSTAGED,
        entries: Optional[int] = None,
        size: Union[int, DataSize, None] = None,
        checksum: Optional[int] = None,
    ) -> File:

        file = File(
            self,
            path,
            status=status,
            location=location,
            stage_status=stage_status,
            entries=entries,
            size=size,
            checksum=checksum,
        )
        self._files[file._dirname][file._name] = file

        return file

    def get_files(self) -> None:
        pass

    @property
    def tree_name(self) -> str:

        return self._tree_name

    @property
    def cross_section(self) -> float:

        if self._cross_section is None:
            raise AttributeError(f"Sample {self} has no cross_section attribute")
        return self._cross_section

    @property
    def data(self) -> bool:

        if self._data is None:
            raise AttributeError(f"Sample {self} has no data attribute")
        return self._data

    def chain(self) -> Any:

        chain = ROOT.TChain(self.tree_name)
        for f in iter(self):
            chain.Add(f.staged_or_url)

        if config.sc.root_cache_size > 0:
            chain.SetCacheSize(config.sc.root_cache_size)

        return chain


class SampleFromDAS(Sample):

    _dasname: str
    _instance: str

    def __init__(
        self,
        path: PurePathOrStr,
        tree_name: str,
        dasname: str,
        instance: str,
        *,
        title: Optional[str] = None,
        cross_section: Optional[float] = None,
        data: bool = False,
    ) -> None:

        super().__init__(
            path,
            tree_name,
            title=title,
            cross_section=cross_section,
            data=data,
        )

        self._dasname = dasname
        if instance:
            self._instance = instance
        else:
            if self._dasname.endswith("/USER"):
                self._instance = "prod/phys03"
            else:
                self._instance = "prod/phys01"

    def __repr__(self) -> str:

        r = f'SampleFromDAS(path="{self}"'
        if self._title is not None:
            r += f', title="{self._title}"'
        r += f', tree_name="{self._tree_name}", #files={len(self)}, size={self.size:.2a}, dasname="{self._dasname}", instance="{self._instance}"'
        if self._cross_section is not None:
            r += f", cross_section={self._cross_section}"
        return r + f", data={self._data})"

    def dasname(self) -> str:

        return self._dasname

    def instance(self) -> str:

        return self._instance

    def get_files(
        self,
    ) -> None:

        cmd = [
            config.bin.dasgoclient,
            "--json",
            f"--query=file dataset={self._dasname} instance={self._instance}",
        ]
        stdout = subprocess.check_output(cmd)

        for item in json.loads(stdout):
            for file_item in item["file"]:
                if file_item["name"] not in self:
                    path = os.path.join(config.site.store_path, file_item["name"][1:])
                    location = (
                        FileFlags.Location.LOCAL
                        if os.path.exists(path)
                        else FileFlags.Location.REMOTE
                    )
                    self.put_file(
                        file_item["name"],
                        location=location,
                        size=int(file_item["size"]),
                        entries=int(file_item["nevents"]),
                        checksum=int(file_item["adler32"], 16),
                    )


class SampleFromFS(Sample):

    _directory: pathlib.Path
    _filter: str

    def __init__(
        self,
        path: PurePathOrStr,
        tree_name: str,
        directory: PathOrStr,
        filter: Optional[str] = None,
        *,
        title: Optional[str] = None,
        cross_section: Optional[float] = None,
        data: bool = False,
    ) -> None:

        super().__init__(
            path,
            tree_name,
            title=title,
            cross_section=cross_section,
            data=data,
        )

        self._directory = pathlib.Path(directory)
        self._filter = filter if filter else "*.root"

    def __repr__(self) -> str:

        r = f'SampleFromFS(path="{self}"'
        if self._title is not None:
            r += f', title="{self._title}"'
        r += f', tree_name="{self._tree_name}", #files={len(self)}, size={self.size:.2a}, directory="{self._directory}", filter="{self._filter}"'
        if self._cross_section is not None:
            r += f", cross_section={self._cross_section}"
        return r + f", data={self._data})"

    def directory(self) -> pathlib.Path:

        return self._directory

    def filter(self) -> str:

        return self._filter

    def get_files(
        self,
    ) -> None:

        eos = str(self._directory).startswith("/eos")
        store = str(self._directory).startswith(
            os.path.join(config.site.store_path, "store")
        )

        for root, _dirs, names, rootfd in os.fwalk(self._directory):
            for name in names:
                if not fnmatch.fnmatch(name, self._filter):
                    continue
                path = os.path.join(root, name)
                size = os.stat(name, dir_fd=rootfd).st_size
                checksum = int(os.getxattr(path, "eos.checksum"), 16) if eos else None
                if store:
                    path = path[len(config.site.store_path) :]

                if path not in self:
                    self.put_file(path, size=size, checksum=checksum)


class SampleGroup(SampleABC):
    """A collection of Samples"""

    _samples: Dict[str, Sample]

    def __init__(
        self, path: PurePathOrStr, samples: Iterable[SampleABC], *, title: str = None
    ) -> None:

        super().__init__(path, title=title)

        self._samples = {}
        for sample in samples:
            if isinstance(sample, Sample):
                self._samples[str(sample)] = sample
            else:
                raise mrtools.MRTError("No nested SampleGroup")

    def __repr__(self) -> str:

        r = f'SampleGroup(path="{self}"'
        if self._title:
            r += f", title={self._title}"
        r += (
            f", #samples={len(self._samples)}, #files={len(self)}, size={self.size:.2a}"
        )
        return r + ")"

    def __getitem__(self, obj) -> File:

        for sample in self._samples.values():
            try:
                return sample[obj]
            except KeyError:
                pass
        raise KeyError(obj)

    def __iter__(self) -> Iterator[File]:

        return itertools.chain.from_iterable(s for s in self._samples.values())

    def __len__(self) -> int:

        return sum(len(s) for s in self._samples.values())

    @property
    def samples(self) -> Dict[str, Sample]:

        return self._samples
