# Overview

[![Release](https://github.com/pennsignals/dsdk/actions/workflows/release.yml/badge.svg)](https://github.com/pennsignals/dsdk/actions/workflows/release.yml)

[![Test](https://github.com/pennsignals/dsdk/actions/workflows/test.yml/badge.svg)](https://github.com/pennsignals/dsdk/actions/workflows/test.yml)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

An opinionated library to help deploy data science projects

- Free software: MIT license

## Install

    pip install "."

## Develop, Lint & Test

Setup:

    python3.10 -m venv .venv

    . .venv/bin/activate
    pip install ".[dev]"
    pre-commit install

Session:

    . .env/bin/activate
    docker-compose up --build postgres &
    ...
    CONFIG=./local/test.yaml ENV=./secrets/example.env pre-commit run --all-files
    ...
    CONFIG=./local/test.yaml ENV=./secrets/example.env git commit -m 'Message'
    ...
    docker-compose down
    deactivate

Rebuild the postgres container and remove the docker volume if the database schema is changed.

## CI/CD Lint & Test:

    docker-compose up --build test &
    ...
    docker-compose down
