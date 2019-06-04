#!/bin/bash
echo "starting thrift file dumper"

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
JAVA=${JAVA_HOME}/bin/java

args=("$@")
outputFile=${args[$(($#-1))]}
for ((i=0; i < $(($#-1)); i++)) {
  ${JAVA} -ea -cp "singer-0.1-SNAPSHOT.jar:lib/*" \
      com.pinterest.singer.tools.ThriftLogDumper \
      --srcLogFile ${args[$i]} \
      --dstLogFile $outputFile
}

