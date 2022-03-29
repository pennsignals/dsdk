# Overview

[![Release](https://github.com/pennsignals/dsdk/workflows/release/badge.svg)](https://github.com/pennsignals/dsdk/actions?query=workflow%3Arelease)

[![Test](https://github.com/pennsignals/dsdk/workflows/test/badge.svg)](https://github.com/pennsignals/dsdk/actions?query=workflow%3Atest)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

An opinionated library to help deploy data science projects

* Free software: MIT license

## Install

    pip install "."

## Develop, Lint & Test

Setup:

    python3.10 -m venv .venv

    . .venv/bin/activate
    pip install ".[all]"
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
