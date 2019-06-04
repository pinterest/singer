#!/bin/sh

mkdir -p /mnt/log/thrift_log_generator/

echo "starting thrift log generator"

# The following configuration will generate 10 rotated sequence of 50MB log files at the speed of
# 1MB/s.
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
JAVA=${JAVA_HOME}/bin/java

${JAVA} -ea -cp "singer-0.1-SNAPSHOT.jar:lib/*" \
    com.pinterest.singer.tools.ThriftLogGenerator \
    --logDir /mnt/log/thrift_log_generator/ \
    --logFileName thrift_test_topic_1.log \
    --maxNumOfMessages -1 \
    --numOfBytesPerLogFile 50000000 \
    --numOfLogFiles 10 \
    --bytesPerSecond 1000000 \
    --payloadSizeInBytes 5000
