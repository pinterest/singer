#!/bin/sh

if [ "$($1 -version)" = "$2" ]
then
	exit 0
fi

echo "Expected thrift with version string '$2'" > /dev/stderr
exit 1
