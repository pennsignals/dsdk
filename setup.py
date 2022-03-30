# -*- coding: utf-8 -*-
"""Data Science Development/Deployment Toolkit."""

from setuptools import find_packages, setup

INSTALL_REQUIRES = (
    (
        "cfgenvy@"
        "git+https://github.com/pennsignals/cfgenvy.git"
        "@1.3.4#egg=cfgenvy"
    ),
    "numpy>=1.15.4",
    "pandas>=0.23.4",
    "pip>=22.0.4",
    "requests>=2.26.0",
    "setuptools>=61.2.0",
    "setuptools_scm[toml]>=6.4.2",
    "wheel>=0.37.1",
)

PYMSSQL_REQUIRES = ("cython>=0.29.21", "pymssql>=2.2.3")

PSYCOPG2_REQUIRES = ("psycopg2-binary>=2.8.6",)

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
    "isort",
    "mypy",
    "pep8-naming",
    "pre-commit",
    "pylint",
    "pytest",
    "pytest-cov",
    "types-pkg-resources",
    "types-pymssql",
    "types-python-dateutil",
    "types-pymssql",
    "types-pyyaml",
    "types-requests",
    "vcrpy",
)

setup(
    extras_require={
        "all": (PSYCOPG2_REQUIRES + PYMSSQL_REQUIRES + TEST_REQUIRES),
        "psycopg2": PSYCOPG2_REQUIRES,
        "pymssql": PYMSSQL_REQUIRES,
        "test": TEST_REQUIRES,
    },
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    packages=find_packages("src"),
    package_dir={"": "src"},
    python_requires=">=3.7",
    use_scm_version={"local_scheme": "dirty-tag"},
)
