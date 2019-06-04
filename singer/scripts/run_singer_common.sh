#!/bin/bash
# Common routines for graph service prod cluster startup scripts.
# Each script should export LOG_PROPERTIES and SERVER_CONFIG.

export JAVA_HOME=/usr/lib/jvm/java-8-oracle
JAVA=${JAVA_HOME}/bin/java

SERVICENAME=singer
LIB=/usr/local/bin/$SERVICENAME
LOG_DIR=/var/log/$SERVICENAME
RUN_DIR=/var/run/$SERVICENAME
PIDFILE=${RUN_DIR}/${SERVICENAME}.pid
CP=${LIB}:${LIB}/*:${LIB}/lib/*
SINGER_CONFIG_PROPERTY=-Dconfig=${SERVER_CONFIG}
if [ -n "$SERVER_CONFIG_DIR" ]; then
  SINGER_CONFIG_PROPERTY=-Dsinger.config.dir=${SERVER_CONFIG_DIR}
fi

# If running on kubernetes enable kubernetes mode
if [ -n "$KUBERENETES" ]; then
	if [[ $KUBERNETES == "true" ]]; then
		# everytime file changes singer will exit and rely on supervisord to restart
		# upon restart the environment variables will get repopulated in the config file
		envsubst < /etc/singer/singer.kubernetes.properties > /etc/singer/singer.properties
		
	fi
fi

DAEMON_OPTS="-server -Xmx800M -Xms800M -verbosegc -Xloggc:${LOG_DIR}/gc.log \
-XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=100 -XX:GCLogFileSize=2M \
-XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:+PrintGCDateStamps -XX:+PrintClassHistogram \
-XX:+UseG1GC -XX:MaxGCPauseMillis=250 -XX:G1ReservePercent=10 -XX:ConcGCThreads=4 \
-XX:ParallelGCThreads=4 -XX:G1HeapRegionSize=8m -XX:InitiatingHeapOccupancyPercent=70 \
-XX:ErrorFile=${LOG_DIR}/jvm_error.log \
-cp ${CP} -Dlog4j.configuration=${LOG_PROPERTIES} ${SINGER_CONFIG_PROPERTY} \
com.pinterest.singer.SingerMain"

ulimit -n 65536

function server_start {
   echo -n "Starting ${NAME}: "
   for pid in `jps | grep SingerMain | cut -d " " -f 1`; do echo "Killing existing singer process ${pid} prior to starting";  kill $pid; done
   mkdir -p ${LOG_DIR}
   chown -R prod:prod ${LOG_DIR}
   chmod 755 ${LOG_DIR}
   mkdir -p ${RUN_DIR}
   touch ${PIDFILE}
   chown -R prod:prod ${RUN_DIR}
   chmod 755 ${RUN_DIR}
   start-stop-daemon --start --quiet --umask 003 --pidfile ${PIDFILE} --make-pidfile \
           --exec ${JAVA} -- ${DAEMON_OPTS} 2>&1 > /dev/null &

   if [ -n "${CPU_AFFINITY}" ]; then
     set_cpu_affinity
   fi

   echo "${NAME} started."
}

function set_cpu_affinity {
    PID=$(/bin/cat ${PIDFILE})
    NUM_OF_CORES=$(/usr/bin/nproc)
    NUM_SINGER_CORES=$(( (NUM_OF_CORES - (NUM_OF_CORES % 5)) / 5 + 1 ))
    START_ID=$(( (NUM_OF_CORES / (NUM_OF_CORES - NUM_SINGER_CORES))))
    END_ID=$((START_ID + NUM_SINGER_CORES - 1 ))
    /usr/bin/taskset -apc ${START_ID}-${END_ID} ${PID}
}

function server_stop {
    echo -n "Stopping ${NAME}: "
    start-stop-daemon --stop --quiet --pidfile ${PIDFILE} --retry=TERM/30/KILL/5
    echo "${NAME} stopped."
    rm -f ${RUN_DIR}/*
}

case "$1" in

    start)
    server_start
    ;;

    stop)
    server_stop
    ;;

    restart)
    server_stop
    server_start
    ;;

esac

exit 0
