#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
JAVA=${JAVA_HOME}/bin/java

${JAVA} -cp "singer-0.1-SNAPSHOT.jar:lib/*" \
      -Dlog4j.configuration=file:../src/main/config/log4j.test.properties \
      com.pinterest.singer.tools.LogFilesMonitor \
      $1

