#!/bin/sh

LIBS=`find ./target -name "*.jar" | grep -v google-collections | grep -v scala-library-2.8.1.jar | grep -v slf4j-simple \
    | grep -v slf4j-jdk14 | grep -v slf4j-api-1.6.1.jar | grep -v log4j-over-slf4j-1.7.2.jar \
    | grep -v slf4j-log4j12-1.4.3.jar | grep -v gson-1.6 | xargs | tr -s " " ":"`

CP=${LIBS}:./target/singer-0.1-SNAPSHOT.jar

java -cp $CP com.pinterest.singer_thrift.client.LogbackThriftLoggerTool
