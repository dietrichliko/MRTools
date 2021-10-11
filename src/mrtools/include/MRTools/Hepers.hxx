#ifndef __MRTOOLS_HELPERS_HXX__
#define __MRTOOLS_HELPERS_HXX__

template <typename T>
T DefineDeltaR(T df, const std::string &name, const std::vector<std::string> &vars)
{
    return df.Define(name, DeltaR, vars);
}

#endif