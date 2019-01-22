# Lintol Doorstep

This is a tool within the Lintol system.

## Installation

We recommend `pipenv`, but any `setuptools` compatible install method should work.

    pipenv shell
    python3 -m pip install -r requirements.txt
    python3 setup.py install

Or, for development,

    pipenv shell
    python3 -m pip install -r requirements-development.txt
    python3 setup.py develop

## Example

From the project root directory.

    ltldoorstep process tests/examples/data/protected_wrecks.geojson src/ltldoorstep_examples/boundary_checker_impr.py  -e dask.threaded

## Running tests

    python3 -m pytest tests
