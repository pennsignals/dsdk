# -*- coding: utf-8 -*-
"""DSDK."""

from setuptools import find_packages, setup

INSTALL_REQUIRES = (
    "configargparse>=1.2.3",
    "numpy>=1.15.4",
    "pandas>=0.23.4",
    "pip>=20.2.4",
    "pyyaml>=5.3.1",
    "setuptools>=50.3.2",
    "wheel>=0.35.1",
)

PYMONGO_REQUIRES = ("pymongo>=3.11.0",)

PYMSSQL_REQUIRES = ("cython>=0.29.21", "pymssql>=2.1.4")

PSYCOPG2_REQUIRES = ("psycopg2-binary>=2.8.6",)

SETUP_REQUIRES = (
    "pytest-runner>=5.2",
    "setuptools_scm[toml]>=4.1.2",
)

TEST_REQUIRES = (
    "astroid",
    "black",
    "coverage[toml]",
    "flake8",
    "flake8-bugbear",
    "flake8-commas",
    "flake8-comprehensions",
    "flake8-docstrings",
    "flake8-logging-format",
    "flake8-mutable",
    "flake8-sorted-keys",
    "isort<=4.2.5",
    "mypy",
    "pep8-naming",
    "pre-commit",
    "pylint",
    "pytest",
    "pytest-cov",
)

setup(
    extras_require={
        "all": PSYCOPG2_REQUIRES
        + PYMONGO_REQUIRES
        + PYMSSQL_REQUIRES
        + TEST_REQUIRES,
        "psycopg2": PSYCOPG2_REQUIRES,
        "pymongo": PYMONGO_REQUIRES,
        "pymssql": PYMSSQL_REQUIRES,
        "test": TEST_REQUIRES,
    },
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    packages=find_packages("src"),
    package_dir={"": "src"},
    python_requires=">=3.7",
    setup_requires=SETUP_REQUIRES,
    tests_require=TEST_REQUIRES,
    use_scm_version={"local_scheme": "dirty-tag"},
)
