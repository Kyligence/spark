#! /bin/bash

CURRENT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
TRY_TIMES=3
TEMP_DIR=`mktemp -d ${CURRENT_DIR}/QUARD_XXXX`
PACKAGE_PATH=$1
ORIGIN_PACKAGE_NAME=${PACKAGE_PATH##*/}

download_package() {
    printf "download package start.\n"
    cd $TEMP_DIR
    wget --tries=$TRY_TIMES $PACKAGE_PATH 
    if [ "$?" -ne 0 ]; then
        printf "Download package failed\n"
        cd $CURRENT_DIR
        exit 1
    fi
    printf "download package done.\n"
    cd $CURRENT_DIR
}

re_archive_package() {
    printf "re-archive package start.\n"
    cd $TEMP_DIR
    pwd
    tar -zxf $ORIGIN_PACKAGE_NAME
    # rename to general test package name
    mv $(ls | grep -v "KE-") Kyligence-Enterprise-Quard
    # copy license
    sshpass -p hadoop scp -o StrictHostKeyChecking=no root@10.1.2.171:/mnt/jenkins/quard_newten/license/LICENSE ./Kyligence-Enterprise-Quard/
    # re-archive package
    tar -zcf Kyligence-Enterprise-Quard.tar.gz Kyligence-Enterprise-Quard
    printf "re-archive package done.\n"
    cd $CURRENT_DIR
}

distribute_package() {
    printf "distribute package start.\n"
    cd $TEMP_DIR
    while [ "$TRY_TIMES" -gt 0 ]
    do
        sshpass -p hadoop scp Kyligence-Enterprise-Quard.tar.gz root@10.1.2.171:/mnt/jenkins/quard_newten/dist/
        if [ "$?" -eq 0 ]; then
            cd $CURRENT_DIR
            break
        else
            let TRY_TIEMS=TRY_TIMES-1
            continue
        fi
    done
    printf "distribute package done.\n"
    cd $CURRENT_DIR
}

download_package $1
re_archive_package
distribute_package

echo "clean"
rm -rf $TEMP_DIR
