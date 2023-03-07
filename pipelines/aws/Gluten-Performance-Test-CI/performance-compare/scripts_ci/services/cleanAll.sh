#!/bin/bash

if [ $# -ne 1 ];then
        echo "Usage: ./cleanAll.sh key_file"
        exit 1
fi

key_file=$1

source ./varStarrocks.conf
./cleanStarrocks.sh "${key_file}"
./cleanGlutenWithCHStandard.sh "${key_file}"
./cleanVanillaSparkOptimized.sh "${key_file}"
