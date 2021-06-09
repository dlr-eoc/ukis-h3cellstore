# import from rust library
from typing import Union

import numpy as np

from .bamboo_h3 import version
from .columnset import ColumnSet

__all__ = [
    "BambooH3Error",
    "is_release_build",
    "typeid_from_numpy_dtype",
]

__version__ = version()


def is_release_build() -> bool:
    from .bamboo_h3 import is_release_build as __bin_is_release_build
    return __bin_is_release_build()


if not is_release_build():
    import warnings

    warnings.warn("bamboo_h3 is not compiled in release mode. Performance will be degraded.", RuntimeWarning)


class BambooH3Error(Exception):
    pass


__TYPE_MAP = {
    "u8": "u8",
    "uint8": "u8",
    "i8": "i8",
    "int8": "i8",
    "u16": "u16",
    "uint16": "u16",
    "i16": "i16",
    "int16": "i16",
    "u32": "u32",
    "uint32": "u32",
    "i32": "i32",
    "int32": "i32",
    "u64": "u64",
    "uint64": "u64",
    "i64": "i64",
    "int64": "i64",
    "f64": "f64",
    "float64": "f64",
    "f32": "f32",
    "float32": "f32",
    "datetime64[s]": "datetime",
}


def typeid_from_numpy_dtype(dtype: Union[np.dtype, str]) -> str:
    """get the bamboo_h3 type id for the given type"""
    dtype = str(dtype).lower()
    try:
        return __TYPE_MAP[dtype]
    except KeyError:
        raise BambooH3Error(f"unsupported datatype: {dtype}, you may need to do a manual conversion")
