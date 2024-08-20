# Overview

[![Release](https://github.com/pennsignals/dsdk/actions/workflows/release.yml/badge.svg)](https://github.com/pennsignals/dsdk/actions/workflows/release.yml)

[![Test](https://github.com/pennsignals/dsdk/actions/workflows/test.yml/badge.svg)](https://github.com/pennsignals/dsdk/actions/workflows/test.yml)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

An opinionated library to help deploy data science projects

- Free software: MIT license

## Install

    pip install "."

## Develop, Lint & Test

Setup virtual environment:

    python3.10 -m venv .venv

Or setup homebrew virtual environment:

    brew install python@3.12
    python3.12 -m venv .venv

Once virtual environment is setup:

    . .venv/bin/activate
    pip install -U pip setuptools wheel
    pip install -e ".[dev]"
    pre-commit install

Session:

    . .venv/bin/activate
    pytest
    ...
    pre-commit run --all-files
    ...
    git commit -m 'Message'
    ...
    deactivate

## CI/CD Lint & Test:

    docker compose up test
    docker compose up pre-commit
    docker compose up build-wheel && docker compose up install-wheel
    ...
    docker compose down
