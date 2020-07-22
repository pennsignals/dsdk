# -*- coding: utf-8 -*-
"""DSDK."""

from setuptools import find_packages, setup

INSTALL_REQUIRES = (
    "configargparse>=0.15.2",
    "numpy>=1.17.0",
    "pip>=19.3.1",
    "pandas>=0.23.4",
    "setuptools>=42.0.2",
    "wheel>=0.33.6",
)

LINT_REQUIRES = (
    "black",
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
    "pytest",  # lint of tests fails without import
)

MONGO_REQUIRES = ("pymongo",)

MSSQL_REQUIRES = ("cython", "pymssql==2.1.4", "sqlalchemy")

POSTGRES_REQUIRES = ("psycopg2",)

SETUP_REQUIRES = ("pytest-runner", "setuptools_scm>=3.3.3")

TEST_REQUIRES = ("coverage", "pytest", "pytest-cov")


setup(
    license="MIT",
    extras_require={
        "all": LINT_REQUIRES
        + MONGO_REQUIRES
        + MSSQL_REQUIRES
        + POSTGRES_REQUIRES
        + TEST_REQUIRES,
        "lint": LINT_REQUIRES,
        "mongo": MONGO_REQUIRES,
        "mssql": MSSQL_REQUIRES,
        "postgres": POSTGRES_REQUIRES,
        "test": TEST_REQUIRES,
    },
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    packages=find_packages("src"),
    package_dir={"": "src"},
    python_requires=">=3.7",
    setup_requires=SETUP_REQUIRES,
    tests_require=LINT_REQUIRES + TEST_REQUIRES,
    use_scm_version={"local_scheme": "dirty-tag"},
    zip_safe=False,
)
