import pathlib
from typing import Any, Dict, List
import logging

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


class EX02Analyzer(mrtools.DFAnalyzer):
    def samples(
        self, sc: mrtools.SamplesCache, options: Dict[str, Any]
    ) -> List[mrtools.SampleABC]:

        return sc.loads(SAMPLES)

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
    cli.run()


if __name__ == "__main__":
    main()
