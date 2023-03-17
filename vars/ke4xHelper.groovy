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
            cd ${remote_dir}
            set -e
            rm -rf dist
            mkdir dist
            cd dist
            cp ${kepackage_dir}/${package_name} .
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

def deployToServerWithCH(hostIp, remote_name, package_name,
                         remote_dir = '/home/step/data/CH/CH_STEP', kepackage_dir = '/home/4xuser/data/devops', ke_workdir = 'Kyligence-Enterprise-4.5-CKstep-do-not-delete') {
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
            cd ${remote_dir}
            rm -f ke_package/K*.tar.gz
            scp 4xuser@10.0.0.28:${kepackage_dir}/${package_name}  ke_package/
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
            """
            retry(3) {
                sh script: """
                    cd /opt/qa_auto
                    python3 excute_test_plan.py 'first' ${productLine} ${versionName} ${planName} ${envStage} ${check_time} ${REMOTE_USERNAME} ${REMOTE_PASSWORD} '10.1.3.18'
                """
            }
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
                <p>It saved in the path of [/home/4xuser/data/devops], please check it and use! </p>
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