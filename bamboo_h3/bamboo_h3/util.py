import pandas as pd
from h3ronpy import util as __util
from types import ModuleType

# just re-export everything from h3ronpy to provide a single API for users
__doc__ = __util.__doc__
__all__ = [
    "range_extender",
]

for __member_name in dir(__util):
    if not __member_name.startswith("_"):
        __member = getattr(__util, __member_name)
        if not isinstance(__member, ModuleType):
            globals()[__member_name] = __member
            __all__.append(__member_name)


def range_extender(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    """
    Make sure that each dataframe uses the full date range (start ... end) for upsampling of missing dates
    this makes sure we have e.g. complete cycles and treat all chunks equally
    also fills NaN values with ffill & bfill (there should not be any Nan values before calling this function)
    :returns: DataFrame with additional rows if complete range was not covered
    """
    index = []
    if (df.index.min() - pd.Timestamp(start, tz="UTC")).days != 0:
        index.append(pd.Timestamp(start, tz="UTC"))
    if (df.index.max() - pd.Timestamp(end, tz="UTC")).days != 0:
        index.append(pd.Timestamp(end, tz="UTC"))

    if index:
        # concat and fillna with closest known value
        return pd.concat([df, pd.DataFrame([], index=index)]).fillna(method="ffill").fillna(method="bfill")
    return df
