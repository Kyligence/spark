#!/bin/bash
#

#说明
show_usage="args: [-p , -n]\
                                  [--package_file=, --namespace=]"
# 参数 - nfs目录下的文件名
#
# 说明：
#   (1)完整的nfs目录为：/data/jenkins-common/newten-cicd/artifacts/dev 此安装包挂载的pv目录为：/data/jenkins-common/
#   (2)比如想使用 /data/jenkins-common/newten-cicd/artifacts/dev/Kyligence-Enterprise-4.5.x_Beta.36.tar.gz
#        传入的参数为 --package_file=newten-cicd/artifacts/dev/Kyligence-Enterprise-4.5.x_Beta.36.tar.gz
#   (3)！！！！注意，使用相对目录！！！！！最前面没有/
opt_package_file=""

# 参数
# 说明：k8s中的命名空间
opt_namespace=""

GETOPT_ARGS=`getopt -o p:n: -al package_file:,namespace: -- "$@"`
eval set -- "$GETOPT_ARGS"
#获取参数
while [ -n "$1" ]
do
        case "$1" in
                -p|--package_file) opt_package_file=$2; shift 2;;
				-n|--namespace) opt_namespace=$2; shift 2;;
                --) break ;;
                *) echo $1,$2,$show_usage; break ;;
        esac
done

if [[ -z $opt_package_file || -z $opt_namespace ]]; then
        echo $show_usage
        exit -1
fi

grep -rl '<<<namespace>>>' ./ | grep -v 'install.sh' | xargs sed -i "s|<<<namespace>>>|$opt_namespace|"

sed -i "s|<<<package_file>>>|$opt_package_file|" ./kyligence-enterprise/kyligence-enterprise-deploy.yaml
#
#
#
echo "[1/2]部署 Kyligence Enterprise"
kubectl apply -f ./kyligence-enterprise \
	&& kubectl -n $opt_namespace wait --for=condition=Available deploy/kyligence-enterprise --timeout=-1s

if [ $? -eq 1 ]
then
	echo "[FAILED]部署 Kyligence Enterprise" && exit 1
fi
echo "[OK]部署 Kyligence Enterprise"
#
#
#

echo "[2/2]导入SSB数据"
pod=$(kubectl get pods -l app=kyligence-enterprise -n $opt_namespace --no-headers | awk '{print $1}')
kubectl exec -it $pod -c kyligence-enterprise -n $opt_namespace -- sh -c 'sh $KYLIN_HOME/bin/sample.sh'
echo "[OK]导入SSB数据"

cat ./banner.txt

echo http://${opt_namespace}.uat.kylincorp.com/kylin
