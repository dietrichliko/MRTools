import collections
import concurrent.futures as futures
import contextlib
import importlib.metadata
import logging
import os
import pathlib
import subprocess
from types import TracebackType
from typing import IO, Any, Dict, List, Optional, Text, Type, Union

import ROOT  # type: ignore
import yaml
from typing_extensions import Literal
from datasize import DataSize

from mrtools.config import Configuration
from mrtools.db import DBEngine
from mrtools.exceptions import MRTError
from mrtools.samples import (
    File,
    Sample,
    SampleABC,
    SampleFromDAS,
    SampleFromFS,
    SampleGroup,
    samples_flatten,
)

PathOrStr = Union[pathlib.Path, str]

log = logging.getLogger(__package__)

config = Configuration()


class SamplesCache(contextlib.AbstractContextManager):

    refresh: bool
    remote: bool
    threads: int

    engine: DBEngine
    _samples: Dict[str, Dict[str, Sample]]

    def __init__(
        self,
        *,
        refresh: bool = False,
        remote: bool = False,
        threads: Optional[int] = None,
        root_threads: Optional[int] = None,
        check_proxy: bool = True,
        proxy_valid: int = 24,
        db_path: Optional[PathOrStr] = None,
        welcome: bool = True,
    ) -> None:

        self.refresh = refresh
        self.remote = remote
        self.threads = config.sc.threads if threads is None else threads
        root_threads = config.sc.root_threads if root_threads is None else root_threads

        if welcome:
            log.info(
                "Modern ROOT Tools for CMS Analysis, Version %s",
                importlib.metadata.version(__package__),
            )

        ROOT.gROOT.SetBatch()
        ROOT.PyConfig.IgnoreCommandLineOptions = True
        ROOT.EnableImplicitMT(root_threads)

        pkg_dir = pathlib.Path(__file__).parent
        ROOT.gSystem.Load(str(pkg_dir / "libMRTools.so"))

        ROOT.gSystem.AddIncludePath(" -I{}/cxx/include")
        ROOT.gROOT.ProcessLine("#include MRTOOLS/Helpers.hxx")

        if check_proxy:
            voms_proxy_init(config.sc.voms_proxy_path, proxy_valid)
            os.environ["X509_USER_PROXY"] = str(config.sc.voms_proxy_path)

        self._samples = collections.defaultdict(dict)

        self.engine = DBEngine(db_path or config.sc.db_path, config.sc.db_sql_echo)

    def __enter__(self) -> "SamplesCache":
        """Context manager enter"""

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> Literal[False]:
        """Context manager exit"""

        return False

    def load(self, sample_file: PathOrStr) -> List[SampleABC]:

        with open(sample_file, "r") as f:
            return self.loads(f)

    def loads(
        self,
        stream: Union[bytes, IO[bytes], Text, IO[Text]],
    ) -> List[SampleABC]:

        data = yaml.safe_load(stream)
        samples = SamplesCache.dict_to_samples(data)

        with self.engine.session() as session:

            if self.refresh:
                samples_to_get = list(samples_flatten(samples))
            else:
                samples_to_get = []
                for sample in samples_flatten(samples):
                    db_sample = session.read_sample(str(sample))
                    if db_sample is None:
                        samples_to_get.append(sample)
                    else:
                        if type(sample) != type(db_sample):
                            log.warning(
                                "Type of sample %s from definition %s and db %s is inconsistent",
                                sample,
                                sample.__class__.__name__,
                                db_sample.__class__.__name__,
                            )
                        sample._files = db_sample._files

            with futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
                f_to_sample = {
                    executor.submit(sample.get_files): sample
                    for sample in samples_to_get
                }
                for future in futures.as_completed(f_to_sample):
                    sample = f_to_sample[future]
                    try:
                        if (exception := future.exception()) is not None:
                            log.error(
                                "get_files for %s raised exceptions %s",
                                str(sample),
                                str(exception),
                            )
                            continue
                    except futures.CancelledError:
                        log.error("get_files for %s was cancelled.")
                        continue

            for sample in samples_to_get:
                session.write_sample(sample)

        log.info(
            "#Samples: %d, #Files: %d, Size: %s",
            sum(s.samples_len() for s in samples),
            sum(len(s) for s in samples),
            "{0:.2a}".format(DataSize(sum(s.size for s in samples))),
        )
        return samples

    @staticmethod
    def dict_to_samples(data: List[Any]) -> List[SampleABC]:
        """Transform yaml dict to samples"""

        samples: List[SampleABC] = []
        for item in data:
            try:
                name = item.pop("name")
            except KeyError:
                log.error(f"Skipping {item} without name attribute")
                continue
            if "samples" in item:
                log.debug("Defining SampleGroup for %s ...", name)
                subsamples = SamplesCache.dict_to_samples(item.pop("samples"))
                try:
                    samples.append(SampleGroup(name, subsamples, **item))
                except TypeError as exc:
                    log.error(f"Skipping {item} with unexpected key %s.", exc)
                    continue
            elif "dasname" in item:
                log.debug("Defining SampleFromDAS for %s ...", name)
                tree_name = item.pop("tree_name")
                dasname = item.pop("dasname")
                instance = item.pop("instance", None)
                try:
                    samples.append(
                        SampleFromDAS(name, tree_name, dasname, instance, **item)
                    )
                except TypeError as exc:
                    log.error(f"Skipping {item} with unexpected key %s.", exc)
                    continue
            elif "directory" in item:
                log.debug("Defining SampleFromFS for %s ...", name)
                tree_name = item.pop("tree_name")
                directory = item.pop("directory")
                filter = item.pop("filter", None)
                try:
                    samples.append(
                        SampleFromFS(name, tree_name, directory, filter, **item)
                    )
                except TypeError as exc:
                    log.error(f"Skipping {item} with unexpected key %s.", exc)
                    continue
            else:
                log.error(
                    "Skipping {item} of unknown type."
                    "(No dasname, directory or samples attribute)."
                )

        return samples

    def faux_stage(self, samples: List[SampleABC]) -> None:

        #        size = sum(s.size for s in samples)

        with futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
            f_to_file: Dict[futures.Future, File] = {}
            for sample in samples:
                f_to_file |= {
                    executor.submit(file.faux_stage): file
                    for file in sample.files_iter(self.remote)
                }
            for future in futures.as_completed(f_to_file):
                if future.cancelled():
                    continue
                file = f_to_file[future]
                if (exception := future.exception()) is not None:
                    log.error(
                        "faux_stage for %s raised exceptions %s",
                        file,
                        exception,
                    )
                    executor.shutdown(wait=True, cancel_futures=True)
                    raise MRTError("Error during staging")


def voms_proxy_init(
    path: PathOrStr,
    min_validity: int = 24,
    validity: int = 192,
    vo: str = "cms",
    rfc: bool = True,
):
    if isinstance(path, str):
        path = pathlib.Path(path)

    if path.is_file():

        new_proxy = False
        cmd = [
            config.bin.voms_proxy_info,
            "--type",
            "--vo",
            "--timeleft",
            "--file",
            str(path),
        ]
        try:
            output = subprocess.check_output(cmd).decode("utf-8").splitlines()
            if rfc and not output[0].startswith("RFC3820 "):
                log.debug("VOMS proxy is not RFC3820 complient.")
                new_proxy = True
            hours = float(output[1].rstrip()) / 3600
            if hours < min_validity:
                log.debug("VOMS proxy has only %5.2f hours left.", hours)
                new_proxy = True
            if output[2].rstrip() != vo:
                log.warning("VOMS proxy has wrong VO %s.", output[3].rstrip())
                new_proxy = True
        except subprocess.CalledProcessError as exc:
            log.debug("Error %d from voms-proxy-info: %s", exc.returncode, exc.output)
            new_proxy = True
        except ValueError:
            log.error("VOMS proxy has not valid time information")
            new_proxy = True
    else:
        new_proxy = True

    if new_proxy:

        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        cmd = [
            config.bin.voms_proxy_init,
            "--rfc",
            "--voms",
            vo,
            "--valid",
            f"{validity}:0",
            "--out",
            str(path),
        ]
        try:
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError as exc:
            log.fatal("Error %s from voms-proxy-init: %s", exc.returncode, exc.output)
            raise MRTError("Error getting new proxy")
