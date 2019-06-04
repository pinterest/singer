#!/bin/bash
# Script to start singer on servers writing to realpin.

# Exit on first error
set -e

export SERVICENAME=singer
export LOG_PROPERTIES=log4j.realpin.properties
export SERVER_CONFIG=singer.realpin.properties

# Hard code the path because monit does not like `dirname $0`
source /mnt/singer/scripts/run_singer_common.sh

exit 0