========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis|
        |
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/dsdk/badge/?style=flat
    :target: https://readthedocs.org/projects/dsdk
    :alt: Documentation Status

.. |travis| image:: https://api.travis-ci.org/pennsignals/dsdk.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/pennsignals/dsdk

.. |version| image:: https://img.shields.io/pypi/v/dsdk.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/dsdk

.. |wheel| image:: https://img.shields.io/pypi/wheel/dsdk.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/dsdk

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/dsdk.svg
    :alt: Supported versions
    :target: https://pypi.org/project/dsdk

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/dsdk.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/dsdk

.. |commits-since| image:: https://img.shields.io/github/commits-since/pennsignals/dsdk/v0.1.0.svg
    :alt: Commits since latest release
    :target: https://github.com/pennsignals/dsdk/compare/v0.1.0...master



.. end-badges

An opinionated library to help deploy data science projects

* Free software: MIT license

Installation
============

::

    pip install dsdk

You can also install the in-development version with::

    pip install https://github.com/pennsignals/dsdk/archive/master.zip


Documentation
=============


https://dsdk.readthedocs.io/


Development
===========

Before you start development you'll need to run::

    pip install pre-commit &&  pre-commit install

To run all the tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
