#!/usr/bin/env bash
SCRIPT_DIR="$(cd $(dirname "$0"); pwd)"

SRC_PATH=${SCRIPT_DIR}/sources
rm -rf ${SRC_PATH} && mkdir ${SRC_PATH}

## download debezium connector
DEBEZIUM_VERSION=${DEBEZIUM_VERSION:-2.7.0}
echo "downloading debezium postgresql connector ${DEBEZIUM_VERSION}..."
DOWNLOAD_URL=https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}.Final/debezium-connector-postgres-${DEBEZIUM_VERSION}.Final-plugin.tar.gz

curl -o ${SRC_PATH}/debezium-connector-postgres.tar.gz ${DOWNLOAD_URL} \
  && tar -xvf ${SRC_PATH}/debezium-connector-postgres.tar.gz -C ${SRC_PATH} \
  && rm ${SRC_PATH}/debezium-connector-postgres.tar.gz