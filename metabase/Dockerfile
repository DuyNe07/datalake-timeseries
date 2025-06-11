ARG metabase_repo=metabase
ARG metabase_version=latest
FROM metabase/${metabase_repo}:${metabase_version} as metabase

FROM openjdk:21-slim-bullseye

ENV FC_LANG en-US LC_CTYPE en_US.UTF-8

ENV MB_DB_FILE="/conf/metabase.db" \
    MB_DB_HOST="0.0.0.0" \
    MB_JETTY_PORT="3030" \
    MB_DB_TYPE="h2" \
    MB_PLUGINS_DIR="/plugins" \
    MB_ENCRYPTION_SECRET_KEY="ThisIsASecretKeyWithMoreThan16Chars"

# dependencies
RUN mkdir -p ${MB_PLUGINS_DIR} && chmod a+rwx ${MB_PLUGINS_DIR} && \
    mkdir -p /conf && chmod a+rwx /conf && \
    apt-get update -yq && apt-get install -yq bash fonts-dejavu-core fonts-dejavu-extra fontconfig curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    curl https://github.com/starburstdata/metabase-driver/releases/download/6.1.0/starburst-6.1.0.metabase-driver.jar -Lo starburst-6.1.0.metabase-driver.jar && \
    mv starburst-6.1.0.metabase-driver.jar ${MB_PLUGINS_DIR} && \
    useradd --shell /bin/bash -m -d /home/metabase metabase

WORKDIR /app
USER metabase

# copy app from the official image
COPY --from=metabase --chown=metabase /app /app

# expose our default runtime port
EXPOSE 3030

# run it
ENTRYPOINT ["/app/run_metabase.sh"]
