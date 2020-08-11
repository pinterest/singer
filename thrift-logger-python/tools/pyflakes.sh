#!/bin/bash
if [ $# -gt 0 ]
then
    INPUT=( $(echo $@ | xargs -n1 readlink -m) )
    DENYLIST=( $(cat testing/pyflakes_denylist.txt | xargs -L1 readlink -m) )
    OLDIFS="$IFS"
    IFS=$'\n'
    FILTER=$(grep -Fxv "${DENYLIST[*]}" <<< "${INPUT[*]}")
    IFS="$OLDIFS"
    pyflakes $FILTER
else
    # We are looking for all py files, and adding the denylist into the mix.
    # Then we filter anything repeated twice (e.g. if you are a python file
    # and you are in the denylist, you'll show up twice)
    # Then we filter everything through ls to take care of files that may be
    # missing.
    # Finally we pipe it all to pyflakes.
    (\
        (find . -name \*\.py && cat testing/pyflakes_denylist.txt) | \
        egrep -v '(.env|.git|./tasks/adhoc/|tools/batch/adhoc|tools/node_modules)' |\
        sort |uniq -u |\
        xargs ls \
    )|xargs pyflakes
fi

exit $?
