# Overview

[![Release](https://github.com/pennsignals/dsdk/workflows/release/badge.svg)](https://github.com/pennsignals/dsdk/actions?query=workflow%3Arelease)

[![Test](https://github.com/pennsignals/dsdk/workflows/test/badge.svg)](https://github.com/pennsignals/dsdk/actions?query=workflow%3Atest)

An opinionated library to help deploy data science projects

* Free software: MIT license

## Install

    pip install "."

## Develop, Lint & Test

Setup:

    python3.7 -m venv .env

    echo "export POSTGRES_HOST=0.0.0.0" >> .env/bin/activate
    . .env/bin/activate
    pip install ".[all]"
    pre-commit install

Session:

    . .env/bin/activate
    docker-compose -f docker-compose.test.yml up postgres &
    ...
    pre-commit run --all-files
    ...
    git commit -m 'Message'
    ...
    docker-compose -f docker-compose.test.yml down
    deactivate

## CI/CD Lint & Test:

    docker-compose -f docker-compose.test.yml up --build
    ...
    docker-compose -f docker-compose.test.yml down
