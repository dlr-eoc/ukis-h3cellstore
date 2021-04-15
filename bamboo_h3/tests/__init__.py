from pathlib import Path

TESTDATA_PATH = Path(__file__).parent / "data"


def is_release_build() -> bool:
    from bamboo_h3 import bamboo_h3 as __bin
    return __bin.is_release_build()
