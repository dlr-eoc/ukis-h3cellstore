
# import from rust library
from .h3cpy import CompactedTable, ClickhouseConnection, version
import pandas as pd

__all__ = [
    "CompactedTable",
    "ClickhouseConnection",
    "poc_some_dataframe"
]

__version__ = version()


# proof of concepts - to be removed later
def poc_some_dataframe():
    return pd.DataFrame({
        "h3index": ClickhouseConnection().poc_some_h3indexes()
    })