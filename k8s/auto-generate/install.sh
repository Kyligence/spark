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
kubectl apply -f ./namespace/namespace.yaml \
	&& kubectl apply -f ./hdfs/hdfs-env-configmap.yaml \
	&& kubectl apply -f ./yarn/yarn-env-configmap.yaml \
	&& kubectl apply -f ./yarn/yarn-xml-configmap.yaml \
	&& kubectl apply -f ./docker-registry-secret.yaml \
	&& kubectl apply -f ./rancher-monitoring/prometheus/jmx-exporter-configmap.yaml \
	&& kubectl apply -f ./rancher-monitoring/prometheus/servicemonitor.yaml \
	&& kubectl apply -f ./nfs-share-storage.yaml
#
#
#
echo "[1/12]部署 Zookeeper"
kubectl apply -f ./zookeeper/zookeeper.yaml
if [ $? -eq 1 ]
then
	echo "[FAILED]部署 Zookeeper" && exit 1
fi

while true
do
    ready=$(kubectl -n $opt_namespace get statefulset zk -o jsonpath="{.status.readyReplicas}")
    if [[ $ready -eq 3 ]];then
        break
    fi
    sleep 3
done
echo "[OK]部署 Zookeeper"
#
#
#
echo "[2/12]部署 Mysql"
kubectl apply -f ./mysql/mysql.yaml \
	&& kubectl -n $opt_namespace wait --for=condition=Available deploy/mysql --timeout=-1s

if [ $? -eq 1 ]
then
	echo "[FAILED]部署 Mysql" && exit 1
fi
echo "[OK]部署 Mysql"
#
#
#
echo "[3/12]部署 HDFS-NameNode"
kubectl apply -f ./hdfs/namenode \
	&& kubectl -n $opt_namespace wait --for=condition=Available deploy/hdfs-namenode --timeout=-1s

if [ $? -eq 1 ]
then
	echo "[FAILED]部署 HDFS-NameNode" && exit 1
fi
echo "[OK]部署 HDFS-NameNode"
#
#
#
echo "[4/12]部署 HDFS-DataNode"
kubectl apply -f ./hdfs/datanode

if [ $? -eq 1 ]
then
	echo "[FAILED]部署 HDFS-DataNode" && exit 1
fi

while true
do
    ready=$(kubectl -n $opt_namespace get statefulset hdfs-datanode -o jsonpath="{.status.readyReplicas}")
    if [[ $ready -eq 3 ]];then
        break
    fi
    sleep 3
done
echo "[OK]部署 HDFS-DataNode"
#
#
#
echo "[5/12]部署 Yarn-HistoryServer"
kubectl apply -f ./yarn/historyserver \
	&& kubectl -n $opt_namespace wait --for=condition=Available deploy/yarn-historyserver --timeout=-1s

if [ $? -eq 1 ]
then
	echo "[FAILED]部署 Yarn-HistoryServer" && exit 1
fi
echo "[OK]部署 Yarn-HistoryServer"
#
#
#
echo "[6/12]部署 Yarn-ResourceManager"
kubectl apply -f ./yarn/resourcemanager \
	&& kubectl -n $opt_namespace wait --for=condition=Available deploy/yarn-resourcemanager --timeout=-1s

if [ $? -eq 1 ]
then
	echo "[FAILED]部署 Yarn-ResourceManager" && exit 1
fi
echo "[OK]部署 Yarn-ResourceManager"
#
#
#
echo "[7/12]部署 Yarn-NodeManager"
kubectl apply -f ./yarn/nodemanager

if [ $? -eq 1 ]
then
	echo "[FAILED]部署 Yarn-NodeManager" && exit 1
fi

while true
do
    ready=$(kubectl -n $opt_namespace get statefulset yarn-nodemanager -o jsonpath="{.status.readyReplicas}")
    if [[ $ready -eq 2 ]];then
        break
    fi
    sleep 3
done
echo "[OK]部署 Yarn-NodeManager"
#
#
#
echo "[8/12]初始化 Hive Metastore 元数据"
kubectl apply -f ./hive/metastore/hive-initschema-job.yaml \
	&& kubectl -n $opt_namespace wait --for=condition=complete job/hive-initschema-job --timeout=-1s

if [ $? -eq 1 ]
then
	echo "[FAILED]初始化 Hive Metastore 元数据" && exit 1
fi
echo "[OK]初始化 Hive Metastore 元数据"
#
#
#
echo "[9/12]部署 Hive Metastore"
kubectl apply -f ./hive/metastore/hive-metastore-deploy.yaml \
	&& kubectl -n $opt_namespace wait --for=condition=Available deploy/hive-metastore --timeout=-1s

if [ $? -eq 1 ]
then
	echo "[FAILED]部署 Hive Metastore" && exit 1
fi
echo "[OK]部署 Hive Metastore"
#
#
#
echo "[10/12]部署 Hive Server2"
kubectl apply -f ./hive/server2/hive-server2.yaml \
	&& kubectl -n $opt_namespace wait --for=condition=Available deploy/hive-server2 --timeout=-1s

if [ $? -eq 1 ]
then
	echo "[FAILED]部署 Hive Server2" && exit 1
fi
echo "[OK]部署 Hive Server2"
#
#
#
echo "[11/12]部署 Kyligence Enterprise"
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

echo "[12/13]导入SSB数据"
pod=$(kubectl get pods -l app=kyligence-enterprise -n $opt_namespace --no-headers | awk '{print $1}')
kubectl exec -it $pod -c kyligence-enterprise -n $opt_namespace -- sh -c 'sh $KYLIN_HOME/bin/sample.sh'
echo "[OK]导入SSB数据"

cat ./banner.txt

echo http://${opt_namespace}.uat.kylincorp.com/kylin
