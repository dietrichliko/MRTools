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
import time
from typing import Any, Dict, Iterable, Iterator, Optional, Union, cast, List

import ROOT  # type: ignore
from datasize import DataSize
from pathmatch import wildmatch

from mrtools.config import Configuration
from mrtools.exceptions import MRTError

log = logging.getLogger(__package__)
config = Configuration()

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

    def __str__(self) -> str:

        return f"{self.status.name},{self.location.name},{self.stage_status.name}"


class File:
    """File as part of sample"""

    _sample: "Sample"
    _dirname: str
    _name: str
    _size: DataSize
    _flags: FileFlags
    _entries: Optional[int]
    _checksum: Optional[int]

    def __init__(
        self,
        sample: "Sample",
        path: PathOrStr,
        *,
        size: Union[int, DataSize],
        status: FileFlags.Status = FileFlags.Status.OK,
        location: FileFlags.Location = FileFlags.Location.LOCAL,
        stage_status: FileFlags.StageStatus = FileFlags.StageStatus.UNSTAGED,
        entries: Optional[int] = None,
        checksum: Optional[int] = None,
    ) -> None:

        self._sample = sample
        dirname, name = os.path.split(path)
        self._dirname = sys.intern(dirname)
        self._name = name
        self._size = DataSize(size) if isinstance(size, int) else size
        self._flags = FileFlags(
            status=status,
            location=location,
            stage_status=stage_status,
        )
        self._entries = entries
        self._checksum = checksum

    def __str__(self) -> str:
        return os.path.join(self._dirname, self._name)

    def __repr__(self) -> str:
        r = f'File(path="{self}", size={self.size:.2a}, state={self._flags}'
        if self._entries is not None:
            r += f", entries={self._entries}"
        if self._checksum is not None:
            r += f", checksum={self._checksum:08X}"

        return r + ")"

    @property
    def size(self) -> DataSize:
        """Size of file in bytes"""

        return self._size

    @property
    def entries(self) -> Optional[int]:
        """Number of entries of the ROOT chain"""

        return self._entries

    @property
    def checksum(self) -> Optional[int]:
        """Adler32 checksum of the file"""

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
                raise MRTError(
                    f"Could not retrieve the checksum metadata for {self}"
                ) from exc

        log.debug("File %s has checksum %x", str(self), checksum)

        return checksum

    def faux_stage(self) -> None:

        stage_path = config.site.file_cache_path / str(self)[1:]
        if stage_path.exists():
            log.debug("Already staged %s", self)
        else:
            log.debug("Stageing %s ...", self)
            self._flags.stage_status = FileFlags.StageStatus.UNSTAGED
            cmd = [config.bin.xrdcp, "--nopbar", "--retry", str(config.sc.xrdcp_retry)]
            if self.checksum is not None:
                cmd += ["--cksum", f"adler32:{self.checksum:08x}"]
            if self._flags.location == FileFlags.Location.LOCAL:
                cmd += ["--xrate-threshold", "1M"]
            else:
                cmd += ["--xrate-threshold", "10K"]
            cmd += [self.url_or_path, str(stage_path)]
            start_time = time.time()
            subprocess.run(cmd, check=True)
            total_time = time.time() - start_time
            rate = f"{DataSize(self.size/total_time):.2A}"
            log.debug("File %s is staged (%sB/sec)", self, rate)

        self._flags.stage_status = FileFlags.StageStatus.STAGED


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
    def path(self) -> pathlib.PurePath:

        return pathlib.PurePath(self._dirname) / self._name

    @property
    def title(self) -> str:

        return self._title if self._title else self._name

    @property
    def entries(self) -> Optional[int]:

        try:
            return sum(map(operator.attrgetter("entries"), iter(self)))
        except TypeError:
            return None

    @property
    def size(self) -> DataSize:

        return DataSize(sum(map(operator.attrgetter("size"), iter(self))))

    @abc.abstractmethod
    def samples_iter(self) -> Iterator["Sample"]:
        pass

    @abc.abstractmethod
    def samples_len(self) -> int:
        pass

    @abc.abstractmethod
    def files_iter(self, remote: bool = False) -> Iterator[File]:
        pass

    @abc.abstractmethod
    def files_len(self, remote: bool = False) -> int:
        pass


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
        r += f', tree_name="{self._tree_name}", #files={len(self)}'
        if (size := self.size) is not None:
            r += f", size={size:.2a}"
        if (entries := self.entries) is not None:
            r += f", entries={entries}"
        if (cross_section := self.cross_section) is not None:
            r += f", cross_section={cross_section}"
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
    def tree_name(self) -> str:

        return self._tree_name

    @property
    def cross_section(self) -> Optional[float]:

        return self._cross_section

    @property
    def data(self) -> bool:

        return self._data

    def samples_iter(self) -> Iterator["Sample"]:
        yield self

    def samples_len(self) -> int:
        return 1

    def files_iter(self, remote: bool = False) -> Iterator[File]:

        file_iter = iter(self)
        while True:
            try:
                file = next(file_iter)
                if file._flags.status == FileFlags.Status.OK and (
                    remote or file._flags.location == FileFlags.Location.LOCAL
                ):
                    yield file
            except StopIteration:
                break

    def files_len(self, remote: bool = False) -> int:

        len = 0
        file_iter = iter(self)
        while True:
            try:
                file = next(file_iter)
            except StopIteration:
                return len
            if file._flags.status == FileFlags.Status.OK and (
                remote or file._flags.location == FileFlags.Location.LOCAL
            ):
                len += 1

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

    def chain(self, remote: bool = False) -> Any:

        chain = ROOT.TChain(self.tree_name)
        for f in self.files_iter(remote):
            chain.Add(f.url_or_path)

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
        if (size := self.size) is not None:
            r += f", size={size:.2a}"
        if (entries := self.entries) is not None:
            r += f", entries={entries}"
        if (cross_section := self.cross_section) is not None:
            r += f", cross_section={cross_section}"
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
        if (size := self.size) is not None:
            r += f", size={size:.2a}"
        if (entries := self.entries) is not None:
            r += f", entries={entries}"
        if (cross_section := self.cross_section) is not None:
            r += f", cross_section={cross_section}"
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
                raise MRTError("No nested SampleGroup")

    def __repr__(self) -> str:

        r = f'SampleGroup(path="{self}"'
        if self._title:
            r += f", title={self._title}"
        r += f", #samples={len(self._samples)}, #files={len(self)}"
        if (size := self.size) is not None:
            r += f", size={size:.2a}"
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

    def samples_iter(self) -> Iterator["Sample"]:

        return iter(self._samples.values())

    def samples_len(self) -> int:
        return len(self._samples)

    def files_iter(self, remote: bool = False) -> Iterator[File]:

        return itertools.chain.from_iterable(
            s.files_iter(remote) for s in self._samples.values()
        )

    def files_len(self, remote: bool = False) -> int:

        return sum(s.files_len(remote) for s in self._samples.values())


def samples_flatten(samples: Iterable[SampleABC]) -> Iterator[Sample]:

    return itertools.chain.from_iterable(s.samples_iter() for s in samples)


def samples_filter(pattern: str, samples: Iterable[SampleABC]) -> List[Sample]:

    the_samples: List[Sample] = []
    for s in samples_flatten(samples):
        if wildmatch.match(pattern, str(s)):
            the_samples.append(s)

    return the_samples
