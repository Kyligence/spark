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
    mv $(ls | grep -v ".tar.gz") Kyligence-Enterprise-Quard
    # copy license
    cp /mnt/jenkins/quard_newten/license/LICENSE ./Kyligence-Enterprise-Quard/
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
        cp Kyligence-Enterprise-Quard.tar.gz /mnt/jenkins/quard_newten/dist/
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

distribute_upgrade_package() {
    round=1
    upgrade_from=$2
    upgrade_to=$3
    all_platforms=$4
    upgrade_package_link=$5
    echo "upgrade_from: ${upgrade_from}"
    echo "upgrade_to: ${upgrade_to}"
    echo "pal: ${all_platforms}"
    echo "upgrade_package_link: ${upgrade_package_link}"
    declare -a platforms=`echo $4 | sed 's/,/ /g' | sed 's/%/\( /g' | sed 's/@/\ \)/g'`
    echo "platforms: ${platforms[@]}"
    [ ! -d $upgrade_from ] && mkdir $upgrade_from
    [ ! -d $upgrade_to ] && mkdir $upgrade_to
    for item in $upgrade_from $upgrade_to
    do
        cd $CURRENT_DIR
        if [ "${round}" -ne 2 ]; then
            package_name="Kyligence-Enterprise-${item}-GA.tar.gz"
            if [[ $item =~ "4.5" ]]; then
                wget --tries=$TRY_TIMES https://package.kyligence.com/newten-cicd/artifacts/4.5.x/GA/Kyligence-Enterprise-${item}-GA.tar.gz -P $item > /dev/null
            else
                wget --tries=$TRY_TIMES https://package.kyligence.com/newten-cicd/artifacts/4.6.x/GA/Kyligence-Enterprise-${item}-GA.tar.gz -P $item > /dev/null
            fi
        else
            package_name=${upgrade_package_link##*/}
            wget --tries=$TRY_TIMES $upgrade_package_link -P $item > /dev/null
        fi
        cd $item
        tar -zxf $package_name
        # rename to general test package name
        mv $(ls | grep -v ".tar.gz") Kyligence-Enterprise-Quard
        # copy license
        cp /mnt/jenkins/quard_newten/license/LICENSE ./Kyligence-Enterprise-Quard/
        tar -zcf Kyligence-Enterprise-Quard.tar.gz Kyligence-Enterprise-Quard
        rm -rf Kyligence-Enterprise-Quard $package_name
        round=$((round+1))
    done

    cd $CURRENT_DIR

    set +e
    for platform in ${platforms[@]}
    do
        for item in $upgrade_from $upgrade_to
        do
            echo "copy package to: /mnt/jenkins/quard_newten/packages/${platform}/${item}"
            rm -rf /mnt/jenkins/quard_newten/packages/${platform}/${item}
            mkdir -p /mnt/jenkins/quard_newten/packages/${platform}/${item}
            cp -f ${item}/Kyligence-Enterprise-Quard.tar.gz /mnt/jenkins/quard_newten/packages/${platform}/${item}
        done
    done
    set -e
    rm -rf $upgrade_from $upgrade_to
}

if [[ "$1" != "UPGRADE" ]]; then
    download_package $1
    re_archive_package
    distribute_package
else
    distribute_upgrade_package $1 $2 $3 $4 $5
fi

echo "clean"
rm -rf $TEMP_DIR
