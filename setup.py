# -*- coding: utf-8 -*-
"""DSDK."""

from setuptools import find_packages, setup

INSTALL_REQUIRES = (
    (
        "cfgenvy@"
        "git+https://github.com/pennsignals/cfgenvy.git"
        "@1.1.0#egg=cfgenvy"
    ),
    "numpy>=1.15.4",
    "pandas>=0.23.4",
    "pip>=21.2.2",
    "setuptools>=57.4.0",
    "wheel>=0.35.1",
)

EPIC_REQUIRES = ("zeep",)

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
    "isort",
    "mypy",
    "pep8-naming",
    "pre-commit",
    "pylint",
    "pytest",
    "pytest-cov",
    "types-pkg-resources",
    "types-python-dateutil",
    "types-pymssql",
    "types-pyyaml",
)

setup(
    entry_points={
        "console_scripts": [
            "epic-notify = dsdk.epic:Notify.main",
            "epic-validate = dsdk.epic:Validate.main",
        ]
    },
    extras_require={
        "all": (
            EPIC_REQUIRES
            + PSYCOPG2_REQUIRES
            + PYMSSQL_REQUIRES
            + TEST_REQUIRES
        ),
        "psycopg2": PSYCOPG2_REQUIRES,
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
