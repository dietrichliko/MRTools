import pathlib
from typing import Any, Dict, List
import logging

import ROOT

import mrtools
import mrtools.clicklog as clicklog

DataFrame = Any

clicklog.basicConfig(
    format="%(asctime)s - %(levelname)s -  %(message)s", datefmt="%y-%m-%d %H:%M:%S"
)

log = logging.getLogger(__package__)

config = mrtools.Configuration()

SAMPLES = """---
- name: /NanoAOD/CompressedStop/stop250-dm10
  title: "m_t = 250 GeV/c^2, #Delta M = 10 GeV/c^2"
  tree_name: Events
  directory: /eos/vbc/experiments/cms/store/user/liko/CompStop/SUS-RunIIAutumn18FSPremix-Stop250-dm10-006-nanoAOD
- name: /NanoAOD/CompressedStop/stop250-dm20
  title: "m_t = 250 GeV/c^2, #Delta M = 20 GeV/c^2"
  tree_name: Events
  directory: /eos/vbc/experiments/cms/store/user/liko/CompStop/SUS-RunIIAutumn18FSPremix-Stop250-dm20-006-nanoAOD
- name: /NanoAOD/CompressedStop/stop600-dm10
  title: "m_t = 600 GeV/c^2, #Delta M = 10 GeV/c^2"
  tree_name: Events
  directory: /eos/vbc/experiments/cms/store/user/liko/CompStop/SUS-RunIIAutumn18FSPremix-Stop600-dm10-006-nanoAOD
- name: /NanoAOD/CompressedStop/stop600-dm20
  title: "m_t = 600 GeV/c^2, #Delta M = 20 GeV/c^2"
  tree_name: Events
  directory: /eos/vbc/experiments/cms/store/user/liko/CompStop/SUS-RunIIAutumn18FSPremix-Stop600-dm20-006-nanoAOD
"""


class EX01Analyzer(mrtools.DFAnalyzer):
    def define_samples(
        self, sc: mrtools.SamplesCache, options: Dict[str, Any]
    ) -> List[mrtools.SampleABC]:

        return sc.loads(SAMPLES)

    def setup(self, df: DataFrame, sample: str, options: Dict[str, Any]) -> None:

        count = "std::count({0}.begin(),{0}.end(),true)"
        df = (
            df.Define("good_Muon", "Muon_pt > 5 && abs(Muon_eta) < 2.")
            .Define("good_nMuon", count.format("good_Muon"))
            .Define("good_Muon_pt", "Muon_pt[good_Muon]")
            .Define("good_Muon_eta", "Muon_eta[good_Muon]")
            .Define("good_Muon_phi", "Muon_phi[good_Muon]")
        )

        # Define good Jets
        df = (
            df.Define("good_Jet", "Jet_pt > 1")
            .Define("good_nJet", count.format("good_Jet"))
            .Define("good_Jet_pt", "Jet_pt[good_Jet]")
            .Define("good_Jet_eta", "Jet_eta[good_Jet]")
            .Define("good_Jet_phi", "Jet_phi[good_Jet]")
        )

        # Calculate DeltaR
        # df = df.Define(
        #     "MuonJet_DR", "DeltaR(good_Muon_eta,good_Jet_eta,good_Muon_phi,good_Jet_phi)"
        # )
        df = ROOT.DefineDeltaR(
            df,
            "MuonJet_DR",
            ["good_Muon_eta", "good_Jet_eta", "good_Muon_phi", "good_Jet_phi"],
        )

        # load histrogram definitions
        dir = pathlib.Path(__file__).parent
        self.histos_load(df, sample, dir / "example01.yaml")


def main():

    config.init()
    analyzer = EX01Analyzer()

    cli = mrtools.AnalyzerCli(analyzer, pathlib.Path(__file__).stem)
    cli.run()


if __name__ == "__main__":
    main()
