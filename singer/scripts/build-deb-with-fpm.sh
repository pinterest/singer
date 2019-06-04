#!/bin/bash

WDIR="singer/target"
CWD=`pwd`

name=singer
version=0.1.0
description="Singer agent which ships logs from host to log repositories."
url="customer_url"
arch="all"
section="misc"
license="Pinterest proprietary"

case $1 in
    clean)
        echo "Cleaning ${WDIR}"
        rm -fr ${WDIR}/build
        ;;
    build)
        rm -f ${WDIR}/build/singer_${version}_all.deb
        rm -f singer_${version}_all.deb

        mkdir -p ${WDIR}/build/

        echo "Create directory structure"
        mkdir -p ${WDIR}/build/mnt/singer

        echo "Extract files into work area"
        tar zxf target/singer-0.1-SNAPSHOT-bin.tar.gz -C ${WDIR}/build/mnt/singer

        echo "Calling fpm to prepare .deb file"

        cd $WDIR/build/

        fpm --verbose \
            -s dir \
            -t deb \
            -n ${name} \
            -v ${version} \
            --deb-user root \
            --deb-group root \
            --description "${description}" \
            --url="${url}" \
            -a ${arch} \
            --category ${section} \
            --vendor "Pinterest" \
            --license "${license}" \
            -m "Pinterest Ops" \
            --prefix="/" \
            mnt

        cd $CWD
        mv ./${WDIR}/build/${name}_${version}_all.deb ${WDIR}/
        ;;
    *)
        echo "$0 build or clean"
        ;;
esac