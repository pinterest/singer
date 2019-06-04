#!/bin/bash

SCRIPT_DIR=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
OPTIMUS_DIR="$SCRIPT_DIR/../../../../"

java8 -ea -cp $OPTIMUS_DIR/singer/target/lib/*:$OPTIMUS_DIR/singer/target/*:$OPTIMUS_DIR/singer/target/test-classes \
     com.pinterest.singer.e2e.SingerEndToEndTest  200 200 500 10

for i in {1..60}; do echo .; sleep 1; done

# Load test with log-file rotation
java8 -ea -cp $OPTIMUS_DIR/singer/target/lib/*:$OPTIMUS_DIR/singer/target/*:$OPTIMUS_DIR/singer/target/test-classes \
          com.pinterest.singer.e2e.SingerEndToEndTest  100000 1000 8000000 60

for i in {1..60}; do echo .; sleep 1; done

# Singer heartbeat test
java8 -ea -cp $OPTIMUS_DIR/singer/target/lib/*:$OPTIMUS_DIR/singer/target/*:$OPTIMUS_DIR/singer/target/test-classes \
                    com.pinterest.singer.e2e.SingerHeartbeatTest 2 2
