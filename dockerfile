ARG PYTHON_VERSION="3.10"


FROM python:${PYTHON_VERSION}-slim-bullseye as binaries
ARG TINI_VERSION=v0.19.0
WORKDIR /tmp
ENV FREETDS /etc/freetds
ENV PATH /root/.local/bin:$PATH
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
COPY freetds.conf /etc/freetds/
RUN \
    chmod +x /usr/bin/tini && \
    apt-get -qq update --fix-missing && \
    apt-get -qq install -y --no-install-recommends git libyaml-dev > /dev/null && \
    apt-get -qq clean && \
    apt-get -qq autoremove -y --purge && \
    rm -rf /var/lib/apt/lists/* \
ENTRYPOINT [ "/usr/bin/tini", "--" ]

FROM binaries as source
COPY . .
RUN \
   pip install -U pip setuptools wheel

FROM source as pre-commit
RUN \
   pip install ".[all]"
CMD pre-commit run --all-files

#FROM source as test
#LABEL name="dsdk.test"
#WORKDIR /tmp
#RUN \
#    pip install ".[all]" && \
#    ln -s /local ./local && \
#    ln -s /secrets ./secrets && \
#    ln -s /model ./model && \
#    ln -s /gold ./gold
#CMD pytest

FROM source as test
RUN \
    pip install ".[all]"
CMD pytest

FROM source as build-wheel
CMD pip wheel --no-deps -w ./dist .

FROM binaries as install-wheel
CMD pip install --find-links=./dist dsdk[all]