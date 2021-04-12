# install the dependencies needed for development and ci by
# collecting them from all relevant files

import subprocess
from pathlib import Path
import sys


def pip_install(packages):
    if packages:
        subprocess.run(["pip", "install", "--upgrade"] + packages, stdout=sys.stdout, stderr=sys.stderr)


if __name__ == '__main__':
    pip_install(["toml", ])

    import toml  # import only after int has been installed

    directory = Path(__file__).parent
    packages = []
    for pkg in toml.load(directory / "pyproject.toml").get("build-system", {}).get("requires"):
        packages.append(pkg)
    for pkg in toml.load(directory / "Cargo.toml").get("package", {}).get("metadata", {}).get("maturin", {}).get(
            "requires-dist", []):
        packages.append(pkg)
    pip_install(packages)
