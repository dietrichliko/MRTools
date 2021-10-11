import abc
import collections
import copy
import itertools
import logging
import pathlib
import time
from typing import IO, Any, Dict, List, Text, Union, Optional

import ROOT  # type: ignore
import yaml
from quantiphy import Quantity

from mrtools.samples import SampleABC, Sample, samples_flatten
from mrtools.samples_cache import SamplesCache

PathOrStr = Union[pathlib.Path, str]
DataFrame = Any

log = logging.getLogger(__package__)

HISTO_OPTS_LABEL = ["xlabel", "ylabel", "logx", "logy", "normalize"]


class Analyzer(abc.ABC):
    @abc.abstractmethod
    def define_samples(
        self, sc: SamplesCache, options: Dict[str, Any]
    ) -> List[SampleABC]:
        pass

    @abc.abstractmethod
    def run(
        self,
        samples: List[SampleABC],
        remote: bool = False,
        options: Dict[str, Any] = None,
    ) -> bool:
        pass

    @abc.abstractmethod
    def save(
        self, output: pathlib.Path, option: Optional[str] = None, plots: bool = False
    ) -> None:
        pass


class SimpleAnalyzer(Analyzer):
    pass


class DFAnalyzer(Analyzer):

    samples: List[Sample]
    chains: Dict[str, Any]
    dataframes: Dict[str, Any]
    events: Dict[str, Any]
    histo_data: Dict[str, Any]
    histos: Dict[str, List[Any]]
    histos_opts: List[Dict[str, Any]]

    def __init__(self) -> None:

        self.chains = {}
        self.dataframes = {}
        self.events = {}
        self.histo_data = {}
        self.histos = collections.defaultdict(list)
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

        if not self.histo_data:
            self.histo_data = yaml.safe_load(stream)
            # TODO (dietrich): Yaml verification

        data = copy.deepcopy(self.histo_data)

        log.debug("Defining histograms for %s", sample)
        if "Histo1D" in data:
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

        if "Histo2D" in data:
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

        if "HProfile1D" in data:
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

    def run(
        self,
        samples: List[SampleABC],
        remote: bool = False,
        options: Dict[str, Any] = None,
    ) -> bool:

        if options is None:
            options = {}

        self.samples = list(samples_flatten(samples))

        for sample in self.samples:
            sname = str(sample)
            self.chains[sname] = sample.chain(remote)
            self.dataframes[sname] = ROOT.RDataFrame(self.chains[sname])
            self.events[sname] = self.dataframes[sname].Count()
            self.setup(self.dataframes[sname], sname, options)

        log.info(
            "Start processing %s samples, %d files...",
            len(samples),
            sum(len(s) for s in samples),
        )
        start_time = time.time()
        try:
            ROOT.RDF.RunGraphs(
                list(self.events.values())
                + list(itertools.chain.from_iterable(self.histos.values()))
            )
        except ROOT.std.runtime_error as exc:
            log.fatal("%s", exc)
            return False
        total_time = time.time() - start_time
        total_events = sum(e.GetValue() for e in self.events.values())
        log.info(
            "Total number of events: %d, Rate %s",
            total_events,
            Quantity(total_events / total_time, "Hz"),
        )
        return True

    def save(
        self, output: pathlib.Path, option: Optional[str] = None, plots: bool = False
    ) -> None:

        # normalize histos

        for sname in self.histos.keys():
            if (events := self.events[sname].GetValue()) <= 0:
                continue
            for i, hist in enumerate(self.histos[sname]):
                if self.histos_opts[i].get("normalize", False):
                    hist.Scale(1.0 / events)

        out = ROOT.TFile(str(output), option or "RECREATE")

        for sname in self.histos.keys():
            key = sname[1:].replace("/", "_")
            out.mkdir(key)
            out.cd(key)
            for histos in self.histos[sname]:
                histos.Write()
            out.cd()

        if plots:
            self.save_plots(out)

        out.Close()

    def save_plots(self, out: Any) -> None:

        for i, histos1D in enumerate(zip(*self.histos.values())):

            name = histos1D[0].GetName()
            title = histos1D[0].GetTitle()
            stackHisto = ROOT.THStack(name, title)

            c = ROOT.TCanvas(name, title, 1000, 1000)

            legend = ROOT.TLegend(0.7, 0.8, 0.88, 0.88)

            if "logx" in self.histos_opts[i]:
                c.SetLogx()
            if "logy" in self.histos_opts[i]:
                c.SetLogy()
            normalize = self.histos_opts[i].get("normalize", None)
            xlabel = self.histos_opts[i].get("xlabel", None)
            ylabel = self.histos_opts[i].get("ylabel", None)
            ylabel = ylabel or "f(x)" if normalize else "Entries"

            for s, h1D in enumerate(histos1D):
                h1D.SetLineColor(s + 2)
                stackHisto.Add(h1D.GetPtr())
                legend.AddEntry(h1D.GetPtr(), self.samples[s].title, "l")

            stackHisto.Draw("NOSTACK HIST")
            stackHisto.GetXaxis().SetTitleOffset(1.3)
            if xlabel:
                stackHisto.GetXaxis().SetTitle(xlabel)
            if ylabel:
                stackHisto.GetYaxis().SetTitle(ylabel)

            legend.Draw()

            c.Write()
