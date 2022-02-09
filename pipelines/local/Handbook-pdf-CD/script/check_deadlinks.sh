#!/bin/bash -il
if [ "$check_deadlinks" == "true" ]; then
    # npm install -g markdown-link-check
    succ=1
    function check_dead_link(){
        for file in `ls $1`
        do
            if [ -d $1"/"$file ]; then
                check_dead_link $1"/"$file $2
            elif [ "${file: 0-3 :3}" == ".md" ]; then
                markdown-link-check $1"/"$file -c check_dead_link_conf.json || succ=0
            fi
        done
    }
    echo "Start to check dead links."
    check_dead_link en
    check_dead_link zh-hans
    if [ $succ -eq 0 ]; then
        echo "Found dead links, please find logs above."
        exit 1
    fi || exit 0;
fi