def call(String ctl){
    println('default entry')
}

def deployToServer(hostIp, remote_name, package_name,
                   remote_dir = '/home/4xuser/data/4.5test', kepackage_dir = '/home/4xuser/data/devops', ke_workdir = 'Kyligence-Enterprise-newten-current') {
    println """deployToServer -> hostIp[${hostIp}],remote_name[${remote_name}],package_name[${package_name}],
        remote_dir[${remote_dir}],kepackage_dir[${kepackage_dir}],ke_workdir[${ke_workdir}]"""
    script {
        def remote = [:]
        remote.name = remote_name
        withCredentials([usernamePassword(credentialsId: 'azure-4xuser', usernameVariable: 'REMOTE_USERNAME', passwordVariable: 'REMOTE_PASSWORD')]) {
            remote.user = REMOTE_USERNAME
            remote.password = REMOTE_PASSWORD
        }
        remote.host = hostIp
        remote.allowAnyHosts = true
        sshCommand remote: remote, command: """
            #! /bin/bash
            set -e
            if test -d ${remote_dir}
            then
                ls -alth ${remote_dir}
            else
                mkdir -p ${remote_dir}
            fi

            cd ${remote_dir}
            rm -rf dist
            mkdir dist
            cd dist

            if test -d ${kepackage_dir}
            then
                echo "【${kepackage_dir}】文件夹存在"
                ls -alth ${kepackage_dir}
            else
                echo "【${kepackage_dir}】文件夹不存在"
                mkdir -p ${kepackage_dir}
            fi

            if [ -f "${kepackage_dir}/${package_name}" ]
            then
                echo "【${kepackage_dir}/${package_name}】本机存在"
                cp ${kepackage_dir}/${package_name} .
            else
                echo "【${kepackage_dir}/${package_name}】本机不存在，尝试从10.0.0.28获取"
                scp 4xuser@10.0.0.28:${kepackage_dir}/${package_name}  .
            fi

            cd ${remote_dir}
            rm -rf new_kedir
            mkdir new_kedir
            tar -zxvf dist/${package_name} --strip-components 1 -C new_kedir
            if test -d ${ke_workdir}
            then
                bash ${ke_workdir}/bin/kylin.sh stop
            else
                echo "The KE server is not running"
            fi
            bash new_kedir/bin/upgrade.sh ${ke_workdir} --silent
            touch ${ke_workdir}/bin/check-env-bypass
            echo "skip check env"
            bash ${ke_workdir}/bin/kylin.sh start
            cat ${ke_workdir}/commit_SHA1
            echo "upgrade ke-env finished"
            """
    }
}

def deployToServerStep(hostIp, remote_name, package_name,
                   remote_dir = '/home/step/data/4.5test', kepackage_dir = '/home/step/data/devops', ke_workdir = 'Kyligence-Enterprise-newten-current') {
    println """deployToServer -> hostIp[${hostIp}],remote_name[${remote_name}],package_name[${package_name}],
        remote_dir[${remote_dir}],kepackage_dir[${kepackage_dir}],ke_workdir[${ke_workdir}]"""
    script {
        def remote = [:]
        remote.name = remote_name
        withCredentials([usernamePassword(credentialsId: 'azure-step', usernameVariable: 'REMOTE_USERNAME', passwordVariable: 'REMOTE_PASSWORD')]) {
            remote.user = REMOTE_USERNAME
            remote.password = REMOTE_PASSWORD
        }
        remote.host = hostIp
        remote.allowAnyHosts = true
        sshCommand remote: remote, command: """
            #! /bin/bash
            set -e
            if test -d ${remote_dir}
            then
                ls -alth ${remote_dir}
            else
                mkdir -p ${remote_dir}
            fi

            cd ${remote_dir}
            rm -rf dist
            mkdir dist
            cd dist

            if test -d ${kepackage_dir}
            then
                echo "【${kepackage_dir}】文件夹存在"
                ls -alth ${kepackage_dir}
            else
                echo "【${kepackage_dir}】文件夹不存在"
                mkdir -p ${kepackage_dir}
            fi

            if [ -f "${kepackage_dir}/${package_name}" ]
            then
                echo "【${kepackage_dir}/${package_name}】本机存在"
                cp ${kepackage_dir}/${package_name} .
            else
                echo "【${kepackage_dir}/${package_name}】本机不存在，尝试从10.0.0.28获取"
                whoami
                scp step@10.0.0.28:${kepackage_dir}/${package_name}  .
                echo "【复制完成】"
            fi
            cd ${remote_dir}
            rm -rf new_kedir
            echo "【delete】"
            mkdir new_kedir
            echo "【mkdir】"
            tar -zxvf dist/${package_name} --strip-components 1 -C new_kedir
            echo "【tar】"
            if test -d ${ke_workdir}
            then
                echo $PWD
                uname -a
                echo `ls ${ke_workdir}`
                echo "【stop ke】"
                set +e
                bash -x ${ke_workdir}/bin/kylin.sh stop
                set -e
                echo "【bash】"
            else
                echo "The KE server is not running"
                echo ${package_name}
                echo `ls`
                tar -zxvf dist/${package_name} -C ${remote_dir}
                echo \$(echo "Kyligence-Enterprise-\\$package_name" | awk -F "-" '{print \$1"-"\$2"-"\$4"-"\$5}')
                mv \$(echo "Kyligence-Enterprise-\\$package_name" | awk -F "-" '{print \$1"-"\$2"-"\$4"-"\$5}') ${ke_workdir}
            fi
            set +e
            bash -x new_kedir/bin/upgrade.sh ${ke_workdir} --silent
            set -e
            echo "【bash2】"
            touch ${ke_workdir}/bin/check-env-bypass
            echo "【touch】"
            echo "skip check env"
            set +e
            mv ${ke_workdir}/lib/ext/mysql-connector-java-5.1.41.jar ${ke_workdir}/lib/ext/mysql-connector-java-5.1.41.jar.bak
            mv ${ke_workdir}/spark/jars/mysql-connector-java-5.1.41.jar ${ke_workdir}/spark/jars/mysql-connector-java-5.1.41.jar.bak
            mv ${ke_workdir}/LICENSE ${ke_workdir}/LICENSE.bak
            set -e
            wget https://repo-ofs.kyligence.com/repository/raw-tar-qa-hosted/libs/mysql-connector-j-8.0.32.jar --directory-prefix=${ke_workdir}/lib/ext/
            wget https://repo-ofs.kyligence.com/repository/raw-tar-qa-hosted/libs/mysql-connector-j-8.0.32.jar --directory-prefix=${ke_workdir}/spark/jars/
            wget https://repo-ofs.kyligence.com/repository/raw-tar-qa-hosted/license/KE4/LICENSE --directory-prefix=${ke_workdir}/

            echo "kylin.metadata.url=step@jdbc,driverClassName=com.mysql.jdbc.Driver,url=jdbc:mysql://10.0.0.43:3306/kylin,username=kylin,password=KyLin@QA_2022
            kylin.env.hdfs-working-dir=/user/step/kylin
            kylin.env.zookeeper-connect-string=10.0.0.7:2181,10.0.0.8:2181,10.0.0.9:2181


            kylin.storage.columnar.spark-conf.spark.yarn.queue=root.step
            kylin.engine.spark-conf.spark.yarn.queue=root.step
            kylin.streaming.spark-conf.spark.yarn.queue=root.step

            kylin.storage.columnar.spark-conf.spark.yarn.am.memory=4g
            kylin.storage.columnar.spark-conf.spark.executor.cores=2
            kylin.storage.columnar.spark-conf.spark.executor.instances=1

            spring.session.store-type=jdbc
            kylin.server.mode=query
            server.port=7072

            #优化策略，定时垃圾清理
            kylin.index.optimization-level=3
            kylin.metadata.ops-cron=0 0/10 * * * ?

            #元数据明文展示
            kylin.metadata.compress.enabled=false
            #不随机密码
            kylin.metadata.random-admin-password.enabled=false


            kylin.streaming.enabled=true
            #streaming
            kylin.streaming.spark-conf.spark.shuffle.io.maxRetries=10
            kylin.streaming.spark-conf.spark.network.timeout=600
            kylin.streaming.spark-conf.spark.driver.memoryOverhead=2048
            kylin.streaming.spark-conf.spark.executor.memoryOverhead=2048
            kylin.streaming.spark-conf.spark.shuffle.io.retryWait=60
            kylin.streaming.spark-conf.spark.executor.heartbeatInterval=300
            kylin.streaming.spark-conf.spark.serializer=org.apache.spark.serializer.KryoSerializer
            kylin.streaming.spark-conf.spark.speculation=true
            kylin.streaming.spark-conf.spark.locality.wait=2s
            kylin.streaming.spark-conf.spark.speculation=true
            kylin.streaming.spark-conf.spark.speculation.interval=900s
            kylin.streaming.spark-conf.spark.speculation.quantile=0.9
            kylin.streaming.spark-conf.spark.speculation.multiplier=1.5
            kylin.streaming.job.retry.interval=5m
            #kylin.streaming.shutdown-timeout=5m
            #kylin.streaming.startup-timeout=5m
            kylin.streaming.kafka.max-rate-per-partition=200
            kylin.streaming.spark-conf.spark.sql.hive.metastore.version=1.2.2
            kylin.streaming.spark-conf.spark.sql.hive.metastore.jars=${ke_workdir}/spark/hive_1_2_2/*
            #kylin.env.zookeeper-max-retries=6
            #kylin.metadata.compress.enabled=false
            #kylin.index.optimization-level=3
            #kylin.query.convert-sum-expression-enabled=false
            #kylin.engine.spark-conf.spark.executor.instances=3" > /tmp/kylin.properties.override.tmp

            cat /tmp/kylin.properties.override.tmp > ${ke_workdir}/conf/kylin.properties.override

            export PATH=$PATH:/usr/bin:/usr/sbin
            if [ ${hostIp} == "159.27.224.211" ]; then
              rm -rf 7000
              rm -rf 7001
              rm -rf 7002
              cp -r ${ke_workdir} 7000
              cp -r ${ke_workdir} 7001
              cp -r ${ke_workdir} 7002
              sed -i 's/sql.hive.metastore.jars=Kyligence-Enterprise-newten-current/sql.hive.metastore.jars=7000/g' 7000/conf/kylin.properties.override
              sed -i 's/sql.hive.metastore.jars=Kyligence-Enterprise-newten-current/sql.hive.metastore.jars=7001/g' 7001/conf/kylin.properties.override
              sed -i 's/sql.hive.metastore.jars=Kyligence-Enterprise-newten-current/sql.hive.metastore.jars=7002/g' 7002/conf/kylin.properties.override
              sed -i 's/server.port=7072/server.port=7000/g' 7000/conf/kylin.properties.override
              sed -i 's/server.port=7072/server.port=7001/g' 7001/conf/kylin.properties.override
              sed -i 's/server.port=7072/server.port=7002/g' 7002/conf/kylin.properties.override
              fuser -k 7000/tcp
              fuser -k 7001/tcp
              fuser -k 7002/tcp
              bash -x 7000/bin/kylin.sh start
              bash -x 7001/bin/kylin.sh start
              bash -x 7002/bin/kylin.sh start
            elif [ ${hostIp} == "159.27.224.168" ]; then
              rm -rf 7003
              rm -rf 7004
              rm -rf 7005
              cp -r ${ke_workdir} 7003
              cp -r ${ke_workdir} 7004
              cp -r ${ke_workdir} 7005
              sed -i 's/sql.hive.metastore.jars=Kyligence-Enterprise-newten-current/sql.hive.metastore.jars=7003/g' 7003/conf/kylin.properties.override
              sed -i 's/sql.hive.metastore.jars=Kyligence-Enterprise-newten-current/sql.hive.metastore.jars=7004/g' 7004/conf/kylin.properties.override
              sed -i 's/sql.hive.metastore.jars=Kyligence-Enterprise-newten-current/sql.hive.metastore.jars=7005/g' 7005/conf/kylin.properties.override
              sed -i 's/server.port=7072/server.port=7003/g' 7003/conf/kylin.properties.override
              sed -i 's/server.port=7072/server.port=7004/g' 7004/conf/kylin.properties.override
              sed -i 's/server.port=7072/server.port=7005/g' 7005/conf/kylin.properties.override
              fuser -k 7003/tcp
              fuser -k 7004/tcp
              fuser -k 7005/tcp
              bash -x 7003/bin/kylin.sh start
              bash -x 7004/bin/kylin.sh start
              bash -x 7005/bin/kylin.sh start
            elif [ ${hostIp} == "159.27.120.134" ]; then
              echo "【start rm】"
              rm -rf 7006
              rm -rf 7007
              rm -rf 7008
              echo "【start cp】"
              cp -r ${ke_workdir} 7006
              cp -r ${ke_workdir} 7007
              cp -r ${ke_workdir} 7008
              echo "【start sed】"
              sed -i 's/sql.hive.metastore.jars=Kyligence-Enterprise-newten-current/sql.hive.metastore.jars=7006/g' 7006/conf/kylin.properties.override
              sed -i 's/sql.hive.metastore.jars=Kyligence-Enterprise-newten-current/sql.hive.metastore.jars=7007/g' 7007/conf/kylin.properties.override
              sed -i 's/sql.hive.metastore.jars=Kyligence-Enterprise-newten-current/sql.hive.metastore.jars=7008/g' 7008/conf/kylin.properties.override
              sed -i 's/server.port=7072/server.port=7006/g' 7006/conf/kylin.properties.override
              sed -i 's/server.port=7072/server.port=7007/g' 7007/conf/kylin.properties.override
              sed -i 's/server.port=7072/server.port=7008/g' 7008/conf/kylin.properties.override
              set +e
              echo "【start fuser 7006】"
              fuser -k 7006/tcp
              echo "【start fuser 7007】"
              fuser -k 7007/tcp
              echo "【start fuser 7008】"
              fuser -k 7008/tcp
              set -e
              echo "【start ke】"
              bash -x 7006/bin/kylin.sh start
              bash -x 7007/bin/kylin.sh start
              bash -x 7008/bin/kylin.sh start
              echo "【end ke】"
            else
              echo "hostIp error : hostIp = ${hostIp}"
              exit 1
            fi
            echo '-----'
            echo pwd
            echo '-----'
            cat ${ke_workdir}/commit_SHA1
            echo '-----'
            echo "upgrade ke-env finished"
            echo '-----'
            """
//             bash -x ${ke_workdir}/bin/kylin.sh start
//             new_ke_workdir=\$(echo "Kyligence-Enterprise-${package_name#*-}" | sed 's/\(^[^-]*-[^-]*-[^-]*-[^-]*\).*$/\1/')
// new_ke_workdir=\$(echo "Kyligence-Enterprise-\\$package_name" | awk -F "-" '{print \$1"-"\$2"-"\$4"-"\$5}')
    }
}

def deployToServerWithCH(hostIp, remote_name, package_name,
                         remote_dir = '/home/step/data/CH/CH_STEP', kepackage_dir = '/home/step/data/devops', ke_workdir = 'Kyligence-Enterprise-4.5-CKstep-do-not-delete') {
    println """deployToServerWithCH -> hostIp[${hostIp}],package_name[${package_name}],
        kepackage_dir[${kepackage_dir}],remote_dir[${remote_dir}],ke_workdir[${ke_workdir}]"""
    script {
        def remote = [:]
        remote.name = remote_name
        withCredentials([usernamePassword(credentialsId: 'azure-step',
                usernameVariable: 'REMOTE_USERNAME',
                passwordVariable: 'REMOTE_PASSWORD')]) {
            remote.user = REMOTE_USERNAME
            remote.password = REMOTE_PASSWORD
        }
        remote.host = hostIp
        remote.allowAnyHosts = true
        sshCommand remote: remote, command: """
            #! /bin/bash
            set -x
            if test -d ${remote_dir}
            then
                ls -alth ${remote_dir}
            else
                mkdir -p ${remote_dir}
            fi
            cd ${remote_dir}
            rm -f ke_package/K*.tar.gz
            scp step@10.0.0.28:${kepackage_dir}/${package_name}  ke_package/
            if test -d new_kedir
            then
                rm -rf new_kedir
                mkdir new_kedir
                tar -xf ke_package/${package_name} -C new_kedir/
            else
                mkdir new_kedir
                tar -xf ke_package/${package_name} -C new_kedir/
            fi
            if test -d ${ke_workdir}
            then
                bash ${ke_workdir}/bin/kylin.sh stop
            else
                echo "The KE server is not running"
            fi
            tmp_ke_dir=`ls new_kedir`
            bash new_kedir/\${tmp_ke_dir}/bin/upgrade.sh ${ke_workdir} --silent
            touch ${ke_workdir}/bin/check-env-bypass
            echo "skip check env"
            cp -rf /home/step/data/CH/bootstrap.sh ${ke_workdir}/sbin/
            echo "replace bootstrap.sh"
            bash ${ke_workdir}/bin/kylin.sh start
            cat ${ke_workdir}/commit_SHA1
            echo "upgrade gw05-168-step-ch-env finished"
        """
    }
}

def stepRun(planName, envStage, productLine, versionName, check_time = 600, time_offset = 0) {
    container('step-build') {
        sleep time_offset
        withCredentials([usernamePassword(credentialsId: 'devops-email-user',
                usernameVariable: 'REMOTE_USERNAME',
                passwordVariable: 'REMOTE_PASSWORD')]) {
            println("replace qa scripts which is from devopslib")
            sh script: """
                cd /opt
                rm -rf *
                ls -lh ${env.WORKSPACE}/qa/4X_STEP_TEST/
                cp -r ${env.WORKSPACE}/qa/4X_STEP_TEST/* /opt
                echo "envStage = ${envStage}"
                echo "check_time = ${check_time}"
                echo "【${productLine} ${versionName} ${planName} ${envStage} ${check_time} ${REMOTE_USERNAME} ${REMOTE_PASSWORD}】"
                cd /opt/qa_auto
                sed -i '1s/^/\\n/' excute_test_plan.py
                sed -i "/except IndexError:/a\\        print(traceback.format_exc())" excute_test_plan.py
                sed -i 's/import urllib3/import urllib3\\nimport traceback/g' excute_test_plan.py
                sed -i 's/+ cluster +/+ f"{cluster}" +/g' excute_test_plan.py
                sed -i 's/, cluster)/, f"{cluster}")/g' excute_test_plan.py
                cat excute_test_plan.py
                python3 -x excute_test_plan.py 'first' ${planName} ${envStage} ${productLine} ${versionName} ${check_time} ${REMOTE_USERNAME} ${REMOTE_PASSWORD} '10.1.3.18'
            """
//             retry(3) {
//                 sh script: """
//                     cd /opt/qa_auto
//                     python3 -x excute_test_plan.py 'first' ${planName} ${envStage} ${productLine} ${versionName} ${check_time} ${REMOTE_USERNAME} ${REMOTE_PASSWORD} '10.1.3.18'
//                 """
//             }
        }
    }
}

def notifyToTester(title, ip, file_name,  emails,failure = false) {
    println """notifyToTester -> title[${title}],ip[${ip}]"""
    def _title = ''
    def _status = failure ? 'FAIL' : 'SUCCESSFUL'
    def _content = """
        <p>[${_status}]: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':</p>
        <p>The Job ${title} the package ${file_name} to the Azure server: [${ip}]</p>
    """
    switch (title) {
        case 'upload':
            _title = 'Upload'
            _content = _content + """
                <p>It saved in the path of [/home/step/data/devops], please check it and use! </p>
            """
            break
        case 'deploy':
            _title = 'Deploy'
            _content = _content + """
                <p>It's run in the url: </p>
                <p><a href="http://${ip}:7072/kylin':''}">http://${ip}:7072/kylin</a></p>"""
            break
        case 'replace':
            _title = 'Replace'
            _content = _content + """
                <p>Replace the CH package and start it, it's run in the url:</p>
                <p><a href="http://${ip}:7072/kylin':''}">http://${ip}:7072/kylin</a></p>
            """
            break
    }

    if (!failure) {
        sendNotify('EMAIL',
                "${_status} to ${_title} into the Azure CN server'",
                emails,
                _content
        )
    }
}

def getPackageInfo(version, envStage, cid, deployType, noSpark = false, customerPkg = 'NORMAL') {
    println "getPackageInfo -> version[${version}],envStage[${envStage}],cid[${cid}],deployType[${deployType}],customerPkg[${customerPkg}]"
    def package_path
    def branchCollection = ['4.6.x', '4.5.x-sp', '4.3.x', '4.3.x-sp', '4.5.x', '4.6.x-sp']
    switch (deployType) {
        case 'Regular':
            package_path = "artifacts/${branchCollection[0]}/${envStage}"
            break
        case 'SP-4.5.x':
            package_path = "artifacts/${branchCollection[1]}/${envStage}"
            break
        case 'KE-4.3.x':
            package_path = "artifacts/${branchCollection[2]}/${envStage}"
            break
        case 'SP-4.3.x':
            package_path = "artifacts/${branchCollection[3]}/${envStage}"
            break
        case 'KE-4.5.x':
            package_path = "artifacts/${branchCollection[4]}/${envStage}"
            break
        case 'SP-4.6.x':
            package_path = "artifacts/${branchCollection[5]}/${envStage}"
            break
    }
    println "package_path: ${package_path}"

    def withoutSpark = noSpark ? '-NoSpark' : ''
    def _customerPkg = ''
    switch (customerPkg) {
        case 'NORMAL':
            _customerPkg = ''
            break
        case 'CUSTOMER_JIANHANG_HDP3':
            _customerPkg = '-CCB'
            break
        case 'CUSTOMER_GUANGDA_CDH5':
            _customerPkg = '-CEB'
            break
    }

    file_name = "KE-${version}-${cid}${withoutSpark}${_customerPkg}.tar.gz"
    package_file = "newten-cicd/${package_path}/${file_name}"
    ga_name = "Kyligence-Enterprise-${version}${withoutSpark}${_customerPkg}.tar.gz"

    println "file_name: ${file_name}"
    println "package_file: ${package_file}"
    println "ga_name: ${ga_name}"

    def obj = [:]
    obj.package_path = package_path
    obj.package_file = package_file
    obj.file_name = file_name
    obj.ga_name = ga_name
    if (envStage == 'GA') {
        obj.file_name = ga_name
        obj.package_file = "newten-cicd/${package_path}/${ga_name}"
    }
    println obj.toString()
    return obj
}