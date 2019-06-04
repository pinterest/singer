#!/bin/bash

TUTORIAL_DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
SINGER_DIR="${TUTORIAL_DIR}/../"

TMP_DIR="/tmp/singer_tutorial"
TMP_KAFKA_DIR="${TMP_DIR}/kafka"
TMP_DATA_DIR="${TMP_DIR}/data"


KAFKA_VERSION=kafka_2.11-2.1.1
HEARTBEAT_TOPIC=singer_heartbeat
SINGER_TUTORIAL_DATA_DIR=/tmp/singer_tutorial/data

PURPLE=`tput setaf 5`
RESET_COLOR=`tput sgr0`

if [ -d ${TMP_KAFKA_DIR}/$KAFKA_VERSION ]
then
    echo "${PURPLE}Kafka binary has already been downloaded ... ${RESET_COLOR}"
else
    echo "Download kafka binary ..."
    curl -L http://apache.mirrors.ionfish.org/kafka/2.1.1/${KAFKA_VERSION}.tgz -o ${TMP_DIR}/kafka.tar.gz
    mkdir -p ${TMP_KAFKA_DIR}
    tar xzf ${TMP_DIR}/kafka.tar.gz --directory ${TMP_KAFKA_DIR}
fi

KAFKA_HOME="${TMP_KAFKA_DIR}/${KAFKA_VERSION}"

echo "${PURPLE}Tail kafka to see Singer heartbeat messages that are sent by Singer ${RESET_COLOR}"
${KAFKA_HOME}/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic ${HEARTBEAT_TOPIC} --from-beginning
