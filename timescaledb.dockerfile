FROM timescale/timescaledb-postgis:latest-pg12 as timescaledb
ARG MONGO_FDW=0
ARG TDS_FDW=1
ARG TDS_FDW_VERSION=2.0.1
ARG MONGO_C_DRIVER_VERSION=1.9.5
# ARG MONGO_C_DRIVER_VERSION=1.15.2
ARG JSON_C_DRIVER_VERSION=0.13.1-20180305
ARG MONGO_FDW_VERSION=5_2_6
WORKDIR \tmp
RUN apk add \
        libsasl \
        openssl \
    && apk add --no-cache --virtual .build \
        autoconf \
        automake \
        clang \
        clang-dev \
        file \
        gcc \
        git \
        libc-dev \
        libtool \
        llvm \
        openssl-dev \
        perl \
        cmake \
        make \
        postgresql-dev \
        sed \
    && if [ "${MONGO_FDW}" = "1" ]; then \
        wget -q "https://github.com/EnterpriseDB/mongo_fdw/archive/REL-${MONGO_FDW_VERSION}.tar.gz" \
        && tar -xvzf REL-${MONGO_FDW_VERSION}.tar.gz \
        && cd mongo_fdw-REL-${MONGO_FDW_VERSION} \
        # && echo '***BEFORE***' \
        # && cat ./autogen.sh \
        && sed -i '18s/.*/MONGOC_VERSION='${MONGO_C_DRIVER_VERSION}'/' ./autogen.sh \
        && sed -i '19s/.*/JSONC_VERSION='${JSON_C_DRIVER_VERSION}'/' ./autogen.sh \
        # && echo '***AFTER***' \
        # && cat ./autogen.sh \
        && . ./autogen.sh --with-master \
        && cd ../ \
        && rm -rf mongo-fdw-REL-${MONGO_FDW_VERSION} \
    ; fi \
    && if [ "${TDS_FDW}" = "1" ]; then \
        apk add --no-cache \
            freetds \
        && apk add --no-cache --virtual .build-mssql \
            freetds-dev \
        && wget -q "https://github.com/tds-fdw/tds_fdw/archive/v${TDS_FDW_VERSION}.tar.gz" \
        && tar -xvzf v${TDS_FDW_VERSION}.tar.gz \
        && cd tds_fdw-${TDS_FDW_VERSION} \
        && make USE_PGXS=1 \
        && make USE_PGXS=1 install \
        # && make TDS_INCLUDE=-I/usr/local/include/ \
        # && make install \
        && cd .. \
        && rm -rf tds_fdw-${TDS_FDW_VERSION} \
        && rm v${TDS_FDW_VERSION}.tar.gz \
        && apk del --no-cache .build-mssql \
    ; fi \
    && apk del --no-cache .build
