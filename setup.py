#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""DSDK."""

import io
import re
from glob import glob
from os.path import basename, dirname, join, splitext

from setuptools import find_packages, setup

CLASSIFIERS = (
    # complete classifier list:
    #    http://pypi.python.org/pypi?%3Aaction=list_classifiers
    "Development Status :: 5 - Production/Stable",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: MIT License",
    "Operating System :: Unix",
    "Operating System :: POSIX",
    "Operating System :: Microsoft :: Windows",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: Implementation :: CPython",
    "Programming Language :: Python :: Implementation :: PyPy",
    "Topic :: Utilities",
)

INSTALL_REQUIRES = (
    "configargparse",
    "pip>=19.3.1",
    "pandas",
    "setuptools>=42.0.2",
)

KEYWORDS = (
    # eg: 'keyword1', 'keyword2', 'keyword3',
)

SETUP_REQUIRES = ("pytest-runner", "setuptools_scm>=3.3.3")

TESTS_REQUIRE = (
    "black",
    "flake8",
    "flake8-bugbear",
    "flake8-commas",
    "flake8-comprehensions",
    "flake8-docstrings",
    "flake8-logging-format",
    "flake8-mutable",
    "flake8-sorted-keys",
    "pep8-naming",
    "pre-commit",
    "pylint",
    "pytest",
    "pytest-cov",
    "pytest-flake8",
    "pytest-mock",
    "pytest-pylint",
)


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


def py_modules():
    """Py Modules."""
    return tuple(splitext(basename(path))[0] for path in glob("src/*.py"))


setup(
    name="dsdk",
    license="MIT",
    description="An opinionated library to help deploy data science projects",
    author="Michael Becker",
    author_email="mike@beckerfuffle.com",
    classifiers=list(CLASSIFIERS),
    py_modules=py_modules(),
    extras_require={
        "test": TESTS_REQUIRE,
        # eg:
        #   'rst': ['docutils>=0.11'],
        #   ':python_version=="2.6"': ['argparse'],
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
    url="https://github.com/pennsignals/dsdk",
    use_scm_version={"fallback_version": "0.1.0", "local_scheme": "dirty-tag"},
    zip_safe=False,
)
