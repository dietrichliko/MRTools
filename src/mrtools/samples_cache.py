import collections
import concurrent.futures as futures
import contextlib
import itertools
import logging
import os
import pathlib
import subprocess
from types import TracebackType
from typing import IO, Any, Dict, Iterable, Iterator, List, Optional, Text, Type, Union

import yaml
from typing_extensions import Literal
import ROOT  # type: ignore

from mrtools.config import Configuration
from mrtools.exceptions import MRTError
from mrtools.samples import Sample, SampleABC, SampleFromDAS, SampleFromFS, SampleGroup

from mrtools.db import DBEngine

PathOrStr = Union[pathlib.Path, str]

log = logging.getLogger(__package__)

config = Configuration()


def samples_iter(samples: Iterable[SampleABC]) -> Iterator[Sample]:

    return itertools.chain.from_iterable(s.samples.values() for s in samples)


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
    ) -> None:

        self.refresh = refresh
        self.remote = remote
        self.threads = config.sc.threads if threads is None else threads
        root_threads = config.sc.root_threads if root_threads is None else root_threads

        ROOT.gROOT.SetBatch()
        ROOT.PyConfig.IgnoreCommandLineOptions = True
        ROOT.EnableImplicitMT(root_threads)

        ROOT.gSystem.Load(str(pathlib.Path(__file__).parent / "libMRTools.so"))

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
                samples_to_get = list(samples_iter(samples))
            else:
                samples_to_get = []
                for sample in samples_iter(samples):
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
