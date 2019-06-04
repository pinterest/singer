#!/bin/bash
# Script to start singer on prod environment.


# Exit on first error
set -e

export SERVICENAME=singer
export LOG_PROPERTIES=log4j.prod.properties
export SERVER_CONFIG_DIR=/etc/singer
# Enable CPU affinity for prod.
export CPU_AFFINITY=1

# Hard code the path because monit does not like `dirname $0`
source /usr/local/bin/singer/scripts/run_singer_common.sh

exit 0
