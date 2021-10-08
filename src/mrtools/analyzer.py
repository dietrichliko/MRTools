import pathlib
import abc
import logging
from typing import Any, Dict, List, Union, IO, Text

import yaml

from mrtools.samples import SampleABC
from mrtools.samples_cache import SamplesCache

PathOrStr = Union[pathlib.Path, str]
DataFrame = Any

log = logging.getLogger(__package__)

HISTO_OPTS_LABEL = ["xlabel", "ylabel", "logx", "logy", "normalize"]


class Analyzer(abc.ABC):
    @abc.abstractmethod
    def samples(self, sc: SamplesCache, options: Dict[str, Any]) -> List[SampleABC]:
        pass


class SimpleAnalyzer(Analyzer):
    pass


class DFAnalyzer(Analyzer):

    histos: Dict[str, List[Any]]
    histos_opts: List[Dict[str, Any]]

    def __init__(self) -> None:

        self.histos_opts = []

    @abc.abstractmethod
    def setup(self, df: DataFrame, sample: str, options: Dict[str, Any]) -> None:
        pass

    def histos_load(self, df: DataFrame, sample: str, path: PathOrStr) -> None:

        with open(path, "r") as f:
            self.histos_loads(df, sample, f)

    def histos_loads(
        self,
        df: DataFrame,
        sample: str,
        stream: Union[bytes, IO[bytes], Text, IO[Text]],
    ) -> None:

        data = yaml.safe_load(stream)
        # TODO (dietrich): Yaml verification

        for h1d in data["Histo1D"]:
            try:
                name = h1d.pop("name")
                title = h1d.pop("title", name)
                nbinx, xmin, xmax = h1d.pop("bins")
                x = h1d.pop("x", name)
                w = h1d.pop("w", None)
            except KeyError as err:
                log.error("Missing histo1D attribute %s", err)
                continue
            opts = {key: h1d.pop(key) for key in HISTO_OPTS_LABEL if key in h1d}

            log.debug("Booking Histo1D %s", name)

            if w is None:
                self.histos[sample].append(
                    df.Histo1D((name, title, nbinx, xmin, xmax), x)
                )
            else:
                # TODO (dietrich): df.Histo1D((name, title, nbinx, xmin, xmax), x, w) does not work
                self.histos[sample].append(
                    df.Histo1D((name, title, nbinx, xmin, xmax), x)
                )
            self.histos_opts.append(opts)

        for h2d in data["Histo2D"]:
            try:
                name = h2d.pop("name")
                title = h2d.pop("title", name)
                nbinx, xmin, xmax, nbiny, ymin, ymax = h2d.pop("bins")
                x = h2d.pop("x")
                y = h2d.pop("y")
                w = h2d.pop("w", None)
            except KeyError as err:
                log.error("Missing Histo2D attribute %s", err)
                continue
            opts = {key: h2d.pop(key) for key in HISTO_OPTS_LABEL if key in h2d}

            log.debug("Booking Histo2D %s", name)

            if w is None:
                self.histos[sample].append(
                    df.Histo2D(
                        (name, title, nbinx, xmin, xmax, nbiny, ymin, ymax), x, y
                    )
                )
            else:
                # TODO (dietrich): df.Histo2D((name, title, nbinx, xmin, xmax, nbiny, ymin, ymax), x, y, w) does not work
                self.histos[sample].append(
                    df.Histo2D(
                        (name, title, nbinx, xmin, xmax, nbiny, ymin, ymax), x, y
                    )
                )
            self.histos_opts.append(opts)

        for hp1d in data["HProfile1D"]:
            try:
                name = hp1d.pop("name")
                title = hp1d.pop("title", name)
                nbinx, xmin, xmax = hp1d.pop("bins")
                x = hp1d.pop("x")
                y = hp1d.pop("y")
                w = hp1d.pop("w", None)
            except KeyError as err:
                log.error("Missing HProfile1D attribute %s", err)
                continue
            opts = {key: hp1d.pop(key) for key in HISTO_OPTS_LABEL if key in hp1d}

            log.debug("Booking HProfile1D %s", name)

            if w is None:
                self.histos[sample].append(
                    df.HProfile1D((name, title, nbinx, xmin, xmax), x, y)
                )
            else:
                # TODO (dietrich): df.HProfile1D((name, title, nbinx, xmin, xmax), x, y, w) does not work
                self.histos[sample].append(
                    df.HProfile1D(
                        (name, title, nbinx, xmin, xmax, nbiny, ymin, ymax), x, y
                    )
                )
            self.histos_opts.append(opts)
