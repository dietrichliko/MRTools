import pathlib
from typing import Any, Dict, List
import logging

import click

import mrtools
import mrtools.clicklog as clicklog

DataFrame = Any

clicklog.basicConfig(
    format="%(asctime)s - %(levelname)s -  %(message)s", datefmt="%y-%m-%d %H:%M:%S"
)

log = logging.getLogger(__package__)

config = mrtools.Configuration()

SAMPLE_NAMES = {
    "2016": "Summer16",
    "2017": "Fall17",
    "2018": "Aumtumn18",
}


class EX02Analyzer(mrtools.DFAnalyzer):
    def samples(
        self, sc: mrtools.SamplesCache, options: Dict[str, Any]
    ) -> List[mrtools.SampleABC]:

        dir = pathlib.Path(__file__).parent
        return sc.load(dir / f"samples/Samples_{SAMPLE_NAMES[options['year']]}.yaml")

    def setup(self, df: DataFrame, sample: str, options: Dict[str, Any]) -> None:

        df = (
            df.Define("good_Muon", "Muon_pt > 5 && abs(Muon_eta) < 2.")
            .Define("good_nMuons", "count(good_Muon)")
            .Define("good_Muon_pt", "Muon_pt[good_Muon]")
            .Define("good_Muon_eta", "Muon_eta[good_Muon]")
            .Define("good_Muon_phi", "Muon_phi[good_Muon]")
        )

        # Define good Jets
        df = (
            df.Define("good_Jet", "Jet_pt > 1")
            .Define("good_nJets", "count(good_Jet)")
            .Define("good_Jet_pt", "Muon_pt[good_Jet]")
            .Define("good_Jet_eta", "Muon_eta[good_Jet]")
            .Define("good_Jet_phi", "Muon_phi[good_Jet]")
        )

        # Calculate DeltaR
        df = df.Define(
            "MuonJet_DR",  # variable to be defined
            "DeltaR(good_Muon_eta,good_Jet_eta,good_Muon_phi,good_Jet_phi)",  # DeltaR signature
        )

        # load histrogram definitions
        dir = pathlib.Path(__file__).parent
        self.histos_load(df, sample, dir / "example02.yaml")


def main():

    config.init()
    analyzer = EX02Analyzer()

    cli = mrtools.AnalyzerCli(analyzer)
    cli.option(
        "--year",
        type=click.Choice(SAMPLE_NAMES.keys()),
        default="2016",
        help="Use data from this year",
    )
    cli.run()


if __name__ == "__main__":
    main()
