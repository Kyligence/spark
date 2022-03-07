#!/bin/bash
#

#说明
show_usage="args: [-n, -v]\
                                  [--namespace=, --version=]"

# 参数
# 说明：k8s中的命名空间
opt_namespace=""

# 参数
# 说明：ke版本号，注意不能带. 例4.5.8 传入--version=458
opt_version=""

GETOPT_ARGS=`getopt -o n:v: -al namespace:,version: -- "$@"`
eval set -- "$GETOPT_ARGS"
#获取参数
while [ -n "$1" ]
do
        case "$1" in
				-n|--namespace) opt_namespace=$2; shift 2;;
				-v|--version) opt_version=$2; shift 2;;
                --) break ;;
                *) echo $1,$2,$show_usage; break ;;
        esac
done

if [[ -z $opt_namespace || -z $opt_version ]]; then
        echo $show_usage
        exit -1
fi