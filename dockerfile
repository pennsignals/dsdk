ARG IFLAGS="--quiet --no-cache-dir --user"

FROM python:3.9.9-slim-bullseye as build
ARG IFLAGS
ARG TINI_VERSION=v0.19.0
WORKDIR /tmp
ENV FREETDS /etc/freetds
ENV PATH /root/.local/bin:$PATH
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
COPY freetds.conf /etc/freetds/
COPY readme.md .
COPY setup.cfg .
COPY setup.py .
COPY pyproject.toml .
COPY .pre-commit-config.yaml .
COPY .git ./.git
COPY src ./src
COPY assets ./assets
COPY test ./test
RUN \
    chmod +x /usr/bin/tini && \
    apt-get -qq update --fix-missing && \
    apt-get -qq install -y --no-install-recommends git libyaml-dev > /dev/null && \
    pip install ${IFLAGS} "." && \
    apt-get -qq clean && \
    apt-get -qq autoremove -y --purge && \
    rm -rf /var/lib/apt/lists/*

FROM python:3.9.9-slim-bullseye as epic
LABEL name="epic"
WORKDIR /tmp
ENV FREETDS /etc/freetds
ENV PATH /root/.local/bin:$PATH
COPY --from=build /root/.local /root/.local
COPY --from=build /tmp/assets /tmp/assets
COPY --from=build /usr/bin/tini /usr/bin
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["epic"]

FROM build as test
ARG IFLAGS
LABEL name="dsdk.test"
WORKDIR /tmp
RUN \
    pip install ${IFLAGS} ".[all]"
ENTRYPOINT [ "/usr/bin/tini", "--" ]
CMD [ "pre-commit", "run", "--all-files" ]
