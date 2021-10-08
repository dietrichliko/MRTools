import logging
import os
import pathlib
import random
import time
from types import TracebackType
from typing import Dict, Optional, Type, Union

import typing_extensions
from sqlalchemy import (
    Boolean,
    Column,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.engine.base import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship

from mrtools.config import Configuration
from mrtools.samples import FileFlags, Sample, SampleFromDAS, SampleFromFS

# type dectaltions
PurePathOrStr = Union[pathlib.PurePath, str]
PathOrStr = Union[pathlib.Path, str]
Base = declarative_base()

log = logging.getLogger(__package__)
config = Configuration()


class DBFile(Base):
    __tablename__ = "file"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    directory_id = Column(Integer, ForeignKey("directory.id"))
    directory = relationship("DBDirectory")
    sample_id = Column(Integer, ForeignKey("sample.id"))
    sample = relationship("DBSample", back_populates="files")
    status = Column(Enum(FileFlags.Status))
    location = Column(Enum(FileFlags.Location))
    stage_status = Column(Enum(FileFlags.StageStatus))
    size = Column(Integer)
    entries = Column(Integer)
    checksum = Column(Integer)

    def __repr__(self) -> str:

        r = f"DBFile(id={self.id:08X)}, name={self.name!r}"
        if self.directory_id is None:
            r += ", directory=None"
        else:
            r += f", directory={self.directory_id:08X}"
        if self.sample_id is None:
            r += ", sample=None"
        else:
            r += f", sample={self.sample_id:08X}"
        if self.size is not None:
            r += f", size={self.size!r}"
        if self.entries is not None:
            r += f", size={self.entries!r}"
        if self.checksum is not None:
            r += f", size={self.checksum:08X}"

        return r + ")"


class DBDirectory(Base):
    __tablename__ = "directory"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    def __repr__(self) -> str:

        return f"DBDirectory(id={self.id:08X}, dirname={self.name!r})"


class DBSample(Base):
    __tablename__ = "sample"

    id = Column(Integer, primary_key=True)
    type = Column(String)
    name = Column(String, nullable=False)
    path_id = Column(Integer, ForeignKey("samplespath.id"))
    path = relationship("DBSamplesPath")
    tree_name = Column(String, nullable=False)
    title = Column(String)
    cross_section = Column(Float)
    data = Column(Boolean, nullable=False)
    files = relationship("DBFile", back_populates="sample")

    __mapper_args__ = {"polymorphic_identity": "sample", "polymorphic_on": type}

    def __repr__(self) -> str:

        return f"DBSample(id={self.id:08X}, name={self.name!r})"


class DBSampleFromDAS(DBSample):
    __tablename__ = "sample_from_das"

    id = Column(Integer, ForeignKey("sample.id"), primary_key=True)
    dasname = Column(String, nullable=False)
    instance = Column(String, nullable=False)

    __mapper_args__ = {
        "polymorphic_identity": "sample_from_das",
    }

    def __repr__(self) -> str:

        return f"DBSampleFromDAS(id={self.id:08X}, name={self.name!r})"  # type: ignore[attr-defined]


class DBSampleFromFS(DBSample):
    __tablename__ = "sample_from_fs"

    id = Column(Integer, ForeignKey("sample.id"), primary_key=True)
    directory = Column(String, nullable=False)
    filter = Column(String, nullable=False)

    __mapper_args__ = {
        "polymorphic_identity": "sample_from_fs",
    }

    def __repr__(self) -> str:

        return f"DBSampleFromFS(id={self.id:08X}, name={self.name!r})"  # type: ignore[attr-defined]


class DBSamplesPath(Base):
    __tablename__ = "samplespath"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    def __repr__(self) -> str:

        return f"DBPath(id={self.id:08X}, path={self.name!r})"


class DBSession:
    """Wrapper class for Session to implement lockfile mechanism"""

    session: Session
    path: Optional[pathlib.Path]

    def __init__(self, engine: Engine, path: Optional[pathlib.Path]) -> None:

        self.session = Session(engine)
        self.path = path

    def __enter__(self) -> "DBSession":
        """Context manager enter"""

        if self.path is not None and config.sc.lockfile:
            log.debug("Creating lockfile")
            lockfile = self.path.with_suffix(".lock")
            icnt = 1
            while True:

                # this assumes that hard links are atomic
                # exponetial backoff, max 128 seconds
                # afterwards check periodically if lockfile
                # is not stale

                try:
                    lockfile.symlink_to(self.path)
                    break
                except OSError:
                    wait_time = random.uniform(0, 2 ** icnt - 1)
                    log.debug("Waiting %5.1f seconds for DB lockfile...", wait_time)
                    time.sleep(wait_time)
                    if icnt < config.sc.lockfile_max_count:
                        icnt += 1
                    else:
                        try:
                            if (
                                age := time.time() - lockfile.lstat().st_mtime
                            ) > config.sc.lockfile_max_age:
                                log.error(
                                    "Lockfile age is %d, older then %d seconds. Removing it.",
                                    age,
                                    config.sc.lockfile_max_age,
                                )
                                lockfile.unlink()
                        except OSError as err:
                            log.error("Error removing lockfile %s", err)

        self.session.begin()

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> typing_extensions.Literal[False]:
        """Context manager exit"""

        self.session.close()  # type: ignore # mypy problem: it has this method

        if self.path is not None and config.sc.lockfile:
            log.debug("Deleting lockfile")
            self.path.with_suffix(".lock").unlink()

        return False

    def read_sample(self, sample_path: PurePathOrStr) -> Optional[Sample]:
        """Read a Sample from DB"""

        dirname, name = os.path.split(sample_path)
        result = (
            self.session.query(DBSample, DBSamplesPath)
            .filter(
                DBSample.path_id == DBSamplesPath.id,
                DBSample.name == name,
                DBSamplesPath.name == dirname,
            )
            .first()
        )
        if result is None:
            return None

        db_sample, _db_samplespath = result

        sample: Sample
        if isinstance(db_sample, DBSampleFromDAS):
            sample = SampleFromDAS(
                sample_path,
                db_sample.tree_name,
                db_sample.dasname,
                db_sample.instance,
                title=db_sample.title,
                cross_section=db_sample.cross_section,
                data=db_sample.data,
            )
        elif isinstance(db_sample, DBSampleFromFS):
            sample = SampleFromFS(
                sample_path,
                db_sample.tree_name,
                db_sample.directory,
                db_sample.filter,
                title=db_sample.title,
                cross_section=db_sample.cross_section,
                data=db_sample.data,
            )
        else:
            sample = Sample(
                sample_path,
                db_sample.tree_name,
                title=db_sample.title,
                cross_section=db_sample.cross_section,
                data=db_sample.data,
            )

        result = self.session.query(DBFile, DBDirectory).filter(
            DBFile.directory_id == DBDirectory.id, DBFile.sample_id == db_sample.id
        )
        for db_file, db_directory in result:
            sample.put_file(
                os.path.join(db_directory.name, db_file.name),
                status=db_file.status,
                location=db_file.location,
                stage_status=db_file.stage_status,
                entries=db_file.entries,
                size=db_file.size,
                checksum=db_file.checksum,
            )

        return sample

    def write_sample(self, sample: Sample) -> None:
        """Write a sample to DB"""

        results = (
            self.session.query(DBSample, DBSamplesPath)
            .filter(
                DBSample.path_id == DBSamplesPath.id,
                DBSample.name == sample._name,
                DBSamplesPath.name == sample._dirname,
            )
            .first()
        )
        if results is None:

            # insert new object

            db_sample: Union[DBSample, DBSampleFromDAS, DBSampleFromFS]
            if isinstance(sample, SampleFromDAS):
                db_sample = DBSampleFromDAS(
                    name=sample._name,
                    title=sample._title,
                    tree_name=sample._tree_name,
                    dasname=sample._dasname,
                    instance=sample._instance,
                    cross_section=sample._cross_section,  # type: ignore
                    data=sample._data,
                )
            elif isinstance(sample, SampleFromFS):
                db_sample = DBSampleFromFS(
                    name=sample._name,
                    title=sample._title,
                    directory=str(sample._directory),
                    filter=sample._filter,
                    tree_name=sample._tree_name,
                    cross_section=sample._cross_section,  # type: ignore
                    data=sample._data,
                )
            else:
                db_sample = DBSample(
                    name=sample._name,
                    title=sample._title,
                    tree_name=sample._tree_name,
                    cross_section=sample._cross_section,  # type: ignore
                    data=sample._data,
                )
            self.session.add(db_sample)

            db_samplespath = (
                self.session.query(DBSamplesPath)
                .filter(DBSamplesPath.name == sample._dirname)
                .first()
            )
            if db_samplespath is None:
                db_samplespath = DBSamplesPath(name=sample._dirname)
                self.session.add(db_samplespath)

            db_sample.path = db_samplespath

            db_files: Dict[str, DBFile] = {}
            db_directories: Dict[str, DBDirectory] = {}

        else:

            # update existing object
            db_sample, db_samplespath = results
            db_sample.title = sample._title
            db_sample.tree_name = sample._tree_name
            db_sample.cross_section = sample._cross_section  # type: ignore
            db_sample.data = sample._data

            results = self.session.query(DBFile, DBDirectory).filter(
                DBFile.directory_id == DBDirectory.id, DBFile.sample_id == db_sample.id
            )

            db_files = {}
            db_directories = {}
            for db_file, db_directory in results:
                path = os.path.join(db_directory.name, db_file.name)
                db_files[path] = db_file
                db_directories[db_directory.name] = db_directory

        for file in sample:

            try:
                db_file = db_files[str(file)]
                db_file.size = file._size
                db_file.entries = file._entries
                db_file.checksum = file._checksum
            except KeyError:
                db_file = DBFile(
                    name=file._name,
                    size=file._size,
                    entries=file._entries,
                    checksum=file._checksum,
                )
                try:
                    db_directory = db_directories[file._dirname]
                except KeyError:
                    db_directory = DBDirectory(name=file._dirname)
                    self.session.add(db_directory)
                    db_directories[file._dirname] = db_directory
                db_file.directory = db_directory
                db_file.sample = db_sample
                self.session.add(db_file)

        for path, db_file in db_files.items():
            name, dirname = os.path.split(path)
            if dirname not in sample._files or name not in sample._files[dirname]:
                self.session.delete(db_file)

        self.session.commit()


class DBEngine:

    engine: Engine
    path: Optional[pathlib.Path]

    def __init__(self, path: PathOrStr, echo: bool = False) -> None:

        print("DB Path", path)
        if path == "":
            self.engine = create_engine(
                "sqlite+pysqlite:///",
                echo=echo,
                future=True,
            )
            self.path = None
        else:
            if isinstance(path, str):
                path = pathlib.Path(path)
            if not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)

            self.engine = create_engine(
                f"sqlite+pysqlite:///{path}",
                echo=echo,
                future=True,
            )
            self.path = path

        Base.metadata.create_all(self.engine)

    def session(self) -> DBSession:

        return DBSession(self.engine, self.path)
