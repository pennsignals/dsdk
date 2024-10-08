ARG PYTHON_VERSION="3.12"
ARG ROOT_CONTAINER=python:${PYTHON_VERSION}-slim-bullseye


FROM ${ROOT_CONTAINER} AS binaries
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
USER root
WORKDIR /tmp
ENV DEBIAN_FRONTEND noninteractive
ENV FREETDS /etc/freetds
COPY freetds.conf /etc/freetds/
RUN \
    apt-get -qq update --yes && \
    apt-get -qq upgrade --yes && \
    apt-get -qq install --yes --no-install-recommends \
        build-essential \
        freetds-dev \
        git \
        libssl-dev \
        libyaml-dev \
        tini \
    > /dev/null && \
    apt-get -qq clean && \
    rm -rf /var/lib/apt/lists/* && \
    pip install -U pip setuptools wheel
ENTRYPOINT ["/usr/bin/tini", "-g", "--"]


FROM binaries AS source
COPY . .


FROM source AS pre-commit
RUN \
    pip install ".[dev]"
CMD pre-commit run --all-files


FROM source AS test
RUN \
    pip install ".[dev]"
CMD pytest


FROM source AS build-wheel
CMD pip wheel --no-deps -w ./dist .


FROM binaries AS install-wheel
CMD pip install --find-links=./dist dsdk[dev]
