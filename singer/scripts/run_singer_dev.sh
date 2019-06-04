#!/bin/bash
# Script to start singer on dev environment.

# Exit on first error
set -e

export LOG_PROPERTIES=log4j.dev.properties
export SERVER_CONFIG=singer.dev.properties

MY_DIR=`dirname $0`
source $MY_DIR/run_singer_common.sh

exit 0