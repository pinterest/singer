#!/bin/bash
if [ $# -gt 0 ]
then
    INPUT=( $(echo $@ | xargs -n1 readlink -m) )
    BLACKLIST=( $(cat testing/pyflakes_blacklist.txt | xargs -L1 readlink -m) )
    OLDIFS="$IFS"
    IFS=$'\n'
    FILTER=$(grep -Fxv "${BLACKLIST[*]}" <<< "${INPUT[*]}")
    IFS="$OLDIFS"
    pyflakes $FILTER
else
    # We are looking for all py files, and adding the blacklist into the mix.
    # Then we filter anything repeated twice (e.g. if you are a python file
    # and you are in the blacklist, you'll show up twice)
    # Then we filter everything through ls to take care of files that may be
    # missing.
    # Finally we pipe it all to pyflakes.
    (\
        (find . -name \*\.py && cat testing/pyflakes_blacklist.txt) | \
        egrep -v '(.env|.git|./tasks/adhoc/|tools/batch/adhoc|tools/node_modules)' |\
        sort |uniq -u |\
        xargs ls \
    )|xargs pyflakes
fi

exit $?
