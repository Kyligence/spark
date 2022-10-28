def deploy_test(deploy_env_host, deploy_home, version) {
    println "deploy_env_host=${deploy_env_host}"
    println "deploy_home=${deploy_home}"
    println "version=${version}"

    def remote = [:]
    remote.name = 'testenv'
    remote.host = deploy_env_host
    remote.user = 'root'
    remote.password = 'notebook'
    remote.allowAnyHosts = true
    remote.pty = true

    sshPut remote: remote, from: "dist/kyligence-Table-Trans-${version}.tar.gz", into: '/tmp'
    sshCommand remote: remote, command: """
if [[ -d ${deploy_home} ]];then
    mkdir ${deploy_home}
fi

if [[ -d ${deploy_home}/kyligence-Table-Trans-${version} ]];then
    bash --login -c '${deploy_home}/kyligence-Table-Trans-${version}/bin/tabletrans.sh stop'
fi

set -e
rm -rf ${deploy_home}/kyligence-Table-Trans-${version}
mv -f /tmp/kyligence-Table-Trans-${version}.tar.gz ${deploy_home}

cd ${deploy_home}
tar -xvf kyligence-Table-Trans-${version}.tar.gz

chmod 777 -R ${deploy_home}/kyligence-Table-Trans-${version}

cd kyligence-Table-Trans-${version}

sed -i 's/# table-trans.data.catalog.aws-glue.url=/table-trans.data.catalog.aws-glue.url=http:\\/\\/datacatalog.cn.kyligence.io/g' conf/tabletrans.override.properties
sed -i 's/# table-trans.data.catalog.default-database=/table-trans.data.catalog.default-database=default_wi7b72z/g' conf/tabletrans.override.properties
sed -i 's/# table-trans.data.catalog.tenant-id=/table-trans.data.catalog.tenant-id=andie123/g' conf/tabletrans.override.properties
sed -i 's/table-trans.metadata.database.port=3306/table-trans.metadata.database.port=3307/g' conf/tabletrans.properties
sed -i 's/table-trans.server.port=9907/table-trans.server.port=9908/g' conf/tabletrans.properties

echo "table-trans.server.storage.upload.dir=/tabletrans/tmp/upload" >> conf/tabletrans.properties
echo "table-trans.server.storage.upload.hive.db=upload_tmp" >> conf/tabletrans.properties
echo "table-trans.server.storage.s3.url=s3a://tabletrans-show-northwest" >> conf/tabletrans.properties
echo "table-trans.server.storage.s3.endpoint=s3.ap-northeast-3.amazonaws.com" >> conf/tabletrans.properties

bash --login -c './bin/tabletrans.sh start'
"""
}

def deploy_prod(deploy_env_host, deploy_home, version) {
    println "deploy_env_host=${deploy_env_host}"
    println "deploy_home=${deploy_home}"
    println "version=${version}"

    def remote = [:]
    remote.name = 'prodtenv'
    remote.host = deploy_env_host
    remote.user = 'root'
    remote.password = 'notebook'
    remote.allowAnyHosts = true
    remote.pty = true

    sshPut remote: remote, from: "dist/kyligence-Table-Trans-${version}.tar.gz", into: '/tmp'
    sshCommand remote: remote, command: """
if [[ -d ${deploy_home} ]];then
    mkdir ${deploy_home}
fi

if [[ -d ${deploy_home}/kyligence-Table-Trans-${version} ]];then
    bash --login -c '${deploy_home}/kyligence-Table-Trans-${version}/bin/tabletrans.sh stop'
fi

set -e
rm -rf ${deploy_home}/kyligence-Table-Trans-${version}
mv -f /tmp/kyligence-Table-Trans-${version}.tar.gz ${deploy_home}
cd ${deploy_home}
tar -xvf kyligence-Table-Trans-${version}.tar.gz
cd kyligence-Table-Trans-${version}

sed -i 's/# table-trans.data.catalog.aws-glue.url=/table-trans.data.catalog.aws-glue.url=http:\\/\\/datacatalog.cn.kyligence.io/g' conf/tabletrans.override.properties
sed -i 's/# table-trans.data.catalog.default-database=/table-trans.data.catalog.default-database=default_wi7b72z/g' conf/tabletrans.override.properties
sed -i 's/# table-trans.data.catalog.tenant-id=/table-trans.data.catalog.tenant-id=andie123/g' conf/tabletrans.override.properties

echo "table-trans.server.storage.upload.dir=/tabletrans/tmp/upload" >> conf/tabletrans.override.properties
echo "table-trans.server.storage.upload.hive.db=upload_tmp" >> conf/tabletrans.override.properties
echo "table-trans.server.storage.s3.url=s3a://tabletrans-show-northwest" >> conf/tabletrans.override.properties
echo "table-trans.server.storage.s3.endpoint=s3.ap-northeast-3.amazonaws.com" >> conf/tabletrans.override.properties

bash --login -c './bin/tabletrans.sh start'
"""
}

def deploy_flag(deploy_env_host, deploy_home, version) {
    println "deploy_env_host=${deploy_env_host}"
    println "deploy_home=${deploy_home}"
    println "version=${version}"

    def remote = [:]
    remote.name = 'flagtenv'
    remote.host = deploy_env_host
    remote.allowAnyHosts = true
    remote.pty = true

    withCredentials([sshUserPrivateKey(credentialsId: 'byzen_azure_flag_key', keyFileVariable: 'identity', passphraseVariable: '', usernameVariable: 'userName')]) {
        remote.user = userName
        remote.identityFile = identity

        sshPut remote: remote, from: "dist/kyligence-Table-Trans-${version}.tar.gz", into: '/tmp'
        sshCommand remote: remote, command: """
if [[ -d ${deploy_home} ]];then
    mkdir ${deploy_home}
fi

if [[ -d ${deploy_home}/kyligence-Table-Trans-${version} ]];then
    bash --login -c '${deploy_home}/kyligence-Table-Trans-${version}/bin/tabletrans.sh stop'
fi

set -e
rm -rf ${deploy_home}/kyligence-Table-Trans-${version}
mv -f /tmp/kyligence-Table-Trans-${version}.tar.gz ${deploy_home}
cd ${deploy_home}
tar -xvf kyligence-Table-Trans-${version}.tar.gz
cd kyligence-Table-Trans-${version}

sed -i 's/# table-trans.metadata.database.username=root/table-trans.metadata.database.username=tabletrans@byzer-cn-mysql-1/g' conf/tabletrans.override.properties
sed -i 's/# table-trans.metadata.database.password=root/table-trans.metadata.database.password=tabletrans_123/g' conf/tabletrans.override.properties
sed -i 's/# table-trans.metadata.database.host=localhost/table-trans.metadata.database.host=byzer-cn-mysql-1.mysql.database.chinacloudapi.cn/g' conf/tabletrans.override.properties

echo "table-trans.server.storage.workdir=/table-trans-data" >> conf/tabletrans.override.properties
echo "table-trans.server.storage.upload.dir=/tabletrans/tmp/upload" >> conf/tabletrans.override.properties
echo "table-trans.server.storage.upload.hive.db=upload_tmp" >> conf/tabletrans.override.properties
echo "table-trans.server.storage.s3.url=/tabletrans/" >> conf/tabletrans.override.properties

bash --login -c './bin/tabletrans.sh start'
"""
    }
}