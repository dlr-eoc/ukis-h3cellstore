
# noinspection PyUnresolvedReferences
from .h3cellstorepy import version

__version__ = version()


def is_release_build() -> bool:
    from .h3cellstorepy import is_release_build as __bin_is_release_build
    return __bin_is_release_build()


if not is_release_build():
    import warnings

    warnings.warn("h3cellstorepy is not compiled in release mode. Performance will be degraded.", RuntimeWarning)

