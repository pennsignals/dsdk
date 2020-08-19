ARG IFLAGS="--quiet --no-cache-dir --user"

FROM python:3.7.7-slim-buster as build
ARG IFLAGS
WORKDIR /root
ENV FREETDS /root/freetds.conf
ENV PATH /root/.local/bin:$PATH
ENV TINI_VERSION v0.16.1
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
COPY freetds.conf .
COPY readme.md .
COPY setup.cfg .
COPY setup.py .
COPY pyproject.toml .
COPY .pre-commit-config.yaml .
COPY .pylintrc .
COPY .git ./.git
COPY src ./src
COPY sql ./sql
COPY test ./test
RUN \
    chmod +x /usr/bin/tini && \
    apt-get -qq update --fix-missing && \
    apt-get -qq install -y --no-install-recommends git > /dev/null && \
    pip install ${IFLAGS} "." && \
    apt-get -qq clean && \
    apt-get -qq autoremove -y --purge && \
    rm -rf /var/lib/apt/lists/*

FROM build as test
ARG IFLAGS
WORKDIR /root
ENV IMAGE dsdk.lint
RUN \
    pip install ${IFLAGS} ".[all]"
ENTRYPOINT [ "/usr/bin/tini", "--" ]
CMD [ "pre-commit", "run", "--all-files" ]
