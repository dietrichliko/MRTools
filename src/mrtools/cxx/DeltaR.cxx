#include "MRTools/DeltaR.hxx"

#include <vector>

VecF_t DeltaR(const VecF_t &eta1, const VecF_t &eta2, const VecF_t &phi1, const VecF_t &phi2)
{

    auto idx = ROOT::VecOps::Combinations(eta1, eta2);

    auto eta1a = Take(eta1, idx[0]);
    auto eta2a = Take(eta2, idx[1]);
    auto phi1a = Take(phi1, idx[0]);
    auto phi2a = Take(phi2, idx[1]);

    return ROOT::VecOps::DeltaR(eta1a, eta2a, phi1a, phi2a);
}


