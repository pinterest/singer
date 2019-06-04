#!/bin/bash

TUTORIAL_DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
SINGER_DIR="${TUTORIAL_DIR}/../"

KAFKA_VERSION=kafka_2.11-2.1.1
TUTORIAL_TOPIC=singer_tutorial
HEARTBEAT_TOPIC=singer_heartbeat
TMP_DIR="/tmp/singer_tutorial"
TMP_KAFKA_DIR="${TMP_DIR}/kafka"
TMP_DATA_DIR="${TMP_DIR}/data"

PURPLE=`tput setaf 5`
RESET_COLOR=`tput sgr0`

mkdir -p ${TMP_DIR}

if [ -d ${TMP_KAFKA_DIR} ]
then
    echo "Directory ${TMP_KAFKA_DIR} has already existed ...."
else
    echo "Download kafka binary ..."
    curl -L http://apache.mirrors.ionfish.org/kafka/2.1.1/kafka_2.11-2.1.1.tgz -o ${TMP_DIR}/kafka.tar.gz
    mkdir -p ${TMP_KAFKA_DIR}
    tar xzf ${TMP_DIR}/kafka.tar.gz --directory ${TMP_KAFKA_DIR}
fi

KAFKA_HOME="${TMP_KAFKA_DIR}/${KAFKA_VERSION}"
ZOOKEEPER_DATA_DIR="/tmp/zookeeper"
KAFKA_DATA_DIR="/tmp/kafka-logs/"

# Tutorial environment cleaning up
echo "Singer tutorial runtime environment setup ... "

# Stop zookeeper process
PIDS=$(ps ax | grep java | grep -i QuorumPeerMain | grep -v grep | awk '{print $1}')

if [ -z "$PIDS" ]; then
  echo "No zookeeper process to stop"
else
  kill -9 $PIDS
fi

# Stop kafka process
PIDS=$(ps ax | grep -i 'kafka\.Kafka' | grep java | grep -v grep | awk '{print $1}')
if [ -z "${PIDS}" ]; then
  echo "No kafka process to stop"
else
  kill -9 ${PIDS}
fi

sleep 2
rm -rf ${TMP_DATA_DIR}
rm -rf ${ZOOKEEPER_DATA_DIR}
rm -rf ${KAFKA_DATA_DIR}


${KAFKA_HOME}/bin/zookeeper-server-start.sh  -daemon  ${KAFKA_HOME}/config/zookeeper.properties
sleep 2

${KAFKA_HOME}/bin/kafka-server-start.sh -daemon   ${TUTORIAL_DIR}/etc/kafka/server.properties
sleep 2

${KAFKA_HOME}/bin/kafka-topics.sh --create --zookeeper localhost:2181  \
       --topic ${TUTORIAL_TOPIC} --partitions 1 --replication-factor  1
sleep 1

${KAFKA_HOME}/bin/kafka-topics.sh --create --zookeeper localhost:2181  \
       --topic ${HEARTBEAT_TOPIC} --partitions 1 --replication-factor  1
sleep 1

# Tutorial environment set up
echo "Setting up tutorial ... "
mkdir -p ${TMP_DATA_DIR}
cp ${TUTORIAL_DIR}/sample_data/*  ${TMP_DATA_DIR}
cp ${TUTORIAL_DIR}/kafka_server.txt ${TMP_DIR}

# Start singer
echo "${PURPLE}Starting Singer process ... ${RESET_COLOR}"
set -x
java -cp ${SINGER_DIR}/singer/target/lib/*:${SINGER_DIR}/singer/target/* \
	 	-Dsinger.config.dir=${TUTORIAL_DIR}/etc/singer  -Dlog4j.configuration=log4j.test.properties \
	 		com.pinterest.singer.SingerMain
