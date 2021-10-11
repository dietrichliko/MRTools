#ifndef __MRTOOLS__DELTAR__
#define __MRTOOLS__DELTAR__

#include "ROOT/RVec.hxx"

using VecF_t = ROOT::VecOps::RVec<float>;

VecF_t DeltaR(const VecF_t &eta1, const VecF_t &eta2, const VecF_t &phi1, const VecF_t &phi2);

#endif