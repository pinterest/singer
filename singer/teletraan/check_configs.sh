#!/bin/bash

SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SINGER_DIR="$( cd $SCRIPTS_DIR/.. && pwd )"
CP=${SINGER_DIR}:${SINGER_DIR}/*:${SINGER_DIR}/lib/*

java -cp ${CP} com.pinterest.singer.tools.LogConfigCheckTool $1