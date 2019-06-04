#!/bin/bash
# Start singer process in foreground with default jvm (installed in docker image)

# Use dev properties by default. They can be overwritten by environment variables (for example pass in by docker run -e) 
LOG_PROPERTIES=${LOG_PROPERTIES:=log4j.dev.properties}
SERVER_CONFIG=${SERVER_CONFIG:=singer.dev.properties}

# These environment variables defaults are redundantly declared for readibility here
# The defaults are overridden by the Dockerfile
KUBERNETES=${KUBERNETES:=false}
KUBERNETES_POD_LOG_DIRECTORY=${KUBERNETES_POD_LOG_DIRECTORY:=/var/log/podlogs}
KUBERNETES_DEFAULT_DELETION_TIMEOUT_SECONDS=${KUBERNETES_DEFAULT_DELETION_TIMEOUT_SECONDS:=120}
KUBERNETES_DELETION_CHECK_INTERVAL_SECONDS=${KUBERNETES_DELETION_CHECK_INTERVAL_SECONDS:=20}
KUBERNETES_POLL_START_DELAY_SECONDS=${KUBERNETES_POLL_START_DELAY_SECONDS:=10}
KUBERNETES_POLL_FREQUENCY_SECONDS=${KUBERNETES_POLL_FREQUENCY_SECONDS:=30}
SINGER_LOG_CONFIG_PATH=${SINGER_LOG_CONFIG_PATH:=conf.d}

# SERVER_CONF_DIR (usually "/etc/singer") will override SERVER_CONFIG
SINGER_CONFIG_PROPERTY=-Dconfig=${SERVER_CONFIG}
if [ -n "$SERVER_CONFIG_DIR" ]; then
  SINGER_CONFIG_PROPERTY=-Dsinger.config.dir=${SERVER_CONFIG_DIR}
fi

# Setup classpath. The scripts folder is where this script exists. The singer folder must be the parent folder of the scripts folder. 
SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SINGER_DIR="$( cd $SCRIPTS_DIR/.. && pwd )"
CP=${SINGER_DIR}:${SINGER_DIR}/*:${SINGER_DIR}/lib/*

# If running on kubernetes enable kubernetes mode
if [ -n "$KUBERNETES" ]; then
	if [[ $KUBERNETES == "true" ]]; then
		echo "Singer Kubernetes mode enabled"
		# everytime file changes singer will exit and rely on supervisord to restart
		# upon restart the environment variables will get repopulated in the config file
		envsubst < /etc/singer/singer.kubernetes.properties > /etc/singer/singer.properties
	fi
fi

# Log path
LOG_DIR=/var/log/singer
mkdir -p $LOG_DIR

echo "Starting Singer"
# JAVA options
JAVA_OPTS="-server -Xmx800M -Xms800M -verbosegc -Xloggc:${LOG_DIR}/gc.log \
	-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=100 -XX:GCLogFileSize=2M \
	-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintClassHistogram \
	-XX:+UseG1GC -XX:MaxGCPauseMillis=250 -XX:G1ReservePercent=10 -XX:ConcGCThreads=4 \
	-XX:ParallelGCThreads=4 -XX:G1HeapRegionSize=8m -XX:InitiatingHeapOccupancyPercent=70 \
	-XX:ErrorFile=${LOG_DIR}/jvm_error.log \
	-cp ${CP} -Dlog4j.configuration=${LOG_PROPERTIES} ${SINGER_CONFIG_PROPERTY} \
	com.pinterest.singer.SingerMain"

# start jvm in foreground
# use exec so that the java process id is same as this script. 
exec java ${JAVA_OPTS}