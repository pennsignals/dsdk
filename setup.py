#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""DSDK."""

import io
import re
from os.path import dirname, join

from setuptools import find_packages, setup

CHECK_REQUIRES = ("docutils", "pygments", "readme-renderer")

CLASSIFIERS = (
    # complete classifier list:
    #    http://pypi.python.org/pypi?%3Aaction=list_classifiers
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Operating System :: Unix",
    "Operating System :: POSIX",
    "Operating System :: Microsoft :: Windows",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: Implementation :: CPython",
    "Topic :: Utilities",
)

DOC_REQUIRES = "sphinx"

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

MSSQL_REQUIRES = ("cython", "pymssql<3.0", "sqlalchemy")

KEYWORDS = (
    # eg: 'keyword1', 'keyword2', 'keyword3',
)

SETUP_REQUIRES = ("pytest-runner", "setuptools_scm>=3.3.3")

TEST_REQUIRES = ("coverage", "pytest", "pytest-cov", "tox")


def read(*names, **kwargs):
    """Read."""
    with io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get("encoding", "utf8"),
    ) as fin:
        return fin.read()


def long_description():
    """Long Description."""
    return "%s\n%s" % (
        re.compile("^.. start-badges.*^.. end-badges", re.M | re.S).sub(
            "", read("README.rst")
        ),
        re.sub(":[a-z]+:`~?(.*?)`", r"``\1``", read("CHANGELOG.rst")),
    )


setup(
    name="dsdk",
    license="MIT",
    description="An opinionated library to help deploy data science projects",
    author="Michael Becker",
    author_email="mike@beckerfuffle.com",
    classifiers=list(CLASSIFIERS),
    extras_require={
        "check": CHECK_REQUIRES,
        "doc": DOC_REQUIRES,
        "lint": LINT_REQUIRES,
        "mongo": MONGO_REQUIRES,
        "mssql": MSSQL_REQUIRES,
        "test": TEST_REQUIRES,
    },
    include_package_data=True,
    install_requires=INSTALL_REQUIRES,
    keywords=list(KEYWORDS),
    long_description=long_description(),
    long_description_content_type="text/x-rst",  # "text/markdown"
    packages=find_packages("src"),
    package_dir={"": "src"},
    project_urls={
        "Changelog": "https://dsdk.readthedocs.io/en/latest/changelog.html",
        "Documentation": "https://dsdk.readthedocs.io/",
        "Issue Tracker": "https://github.com/pennsignals/dsdk/issues",
    },
    python_requires=">=3.7",
    setup_requires=SETUP_REQUIRES,
    tests_require=LINT_REQUIRES + TEST_REQUIRES,
    url="https://github.com/pennsignals/dsdk",
    use_scm_version={"local_scheme": "dirty-tag"},
    zip_safe=False,
)
