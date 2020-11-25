# Overview

![Docker Images](https://github.com/pennsignals/dsdk/workflows/.github/workflows/release.yml/badge.svg)

![Test](https://github.com/pennsignals/dsdk/workflows/.github/workflows/test.yml/badge.svg)

An opinionated library to help deploy data science projects

* Free software: MIT license

## Install

    pip install "."

You may also install the development dependencies with:

    pip install ".[all]"

## Develop

To lint:

    pre-commit install
    pre-commit run --all-files

To lint with docker:

    docker-compose -f docker-compose.lint.yml up --build
    docker-compose -f docker-compose.lint.yml down

To test:

    pytest

To test with docker:

    docker-compose -f docker-compose.test.yml up --build
    docker-compose -f docker-compose.test.yml down
