#!/bin/bash
DENYLIST_FILE="testing/pep8_denylist.txt"
LINTER="pep8 --max-line-length=120"
if [ $# -gt 0 ]
then
    INPUT=( $(echo $@ | xargs -n1 readlink -m) )
    DENYLIST=( $(cat $DENYLIST_FILE | xargs -L1 readlink -m) )
    OLDIFS="$IFS"
    IFS=$'\n'
    FILTER=$(grep -Fxv "${DENYLIST[*]}" <<< "${INPUT[*]}")
    IFS="$OLDIFS"
    # We only need to run pep8 against the specific file if
    # the file is not in DENYLIST (FILTER not empty).
    # If FILTER is empty, we don't need to do anything because
    # all of the provided files are in the denylist.
    if [ -n "$FILTER" ]; then
        $LINTER $FILTER
    fi
else
    # We are looking for all py files, and adding the denylist into the mix.
    # Then we filter anything repeated twice (e.g. if you are a python file
    # and you are in the denylist, you'll show up twice)
    # Then we filter everything through ls to take care of files that may be
    # missing.
    # Finally we pipe it all to pyflakes.
    (\
        (find . -name \*\.py && cat $DENYLIST_FILE) | \
        egrep -v '(.env|.git|./tasks/adhoc/|tools/batch/adhoc|tools/node_modules)' |\
        sort |uniq -u |\
        xargs ls \
    )|xargs $LINTER
fi

exit $?
