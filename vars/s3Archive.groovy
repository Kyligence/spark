def call(String type) {
    if (type == 'upload') {
        uploadArchive()
    } else if (type == 'download') {
        downloadArchive()
    }
}

def uploadArchive() {
    timestamps {
        try {
            withAWS(region: 'us-west-2', credentials: 'aws_global_s3_cp') {
                // Upload reports for analyze
                sh script: """
                    mkdir -p ${BUILD_NUMBER}/report
                    mkdir -p ${BUILD_NUMBER}/site
                    mkdir -p ${BUILD_NUMBER}/exec
                    outputs=\$(find "\$(pwd)" -path '*surefire-reports/*.xml' | sed 's/.*/&/')
                    for out in \$outputs; do
                        cp \$out ${BUILD_NUMBER}/report/
                    done

                    i=1
                    outputs=\$(find "\$(pwd)" -path '*Test-Stage-*site' | sed 's/.*/&/')
                    for out in \$outputs; do
                        target_out=\$(echo \$out | rev |cut -d/ -f 3 | rev)
                        cp -r \$out ${BUILD_NUMBER}/site/"\$target_out-\$i"
                        i=\$((i+1))
                    done
                    outputs=\$(find "\$(pwd)" -path '*Test-Stage-*jacoco.exec' | sed 's/.*/&/')
                    for out in \$outputs; do
                        target_out=\$(echo \$out | rev |cut -d/ -f 3 | rev)
                        cp -r \$out ${BUILD_NUMBER}/exec/"\$target_out-\$i-jacoco.exec"
                        i=\$((i+1))
                    done
                    tar czf ${BUILD_NUMBER}.tar.gz ${BUILD_NUMBER}/
                """
                s3Upload(bucket: 'k8s-bucket-devops', path: "ke-ci-result/${JOB_NAME}/", includePathPattern: "${BUILD_NUMBER}.tar.gz")
            }
            if (defaultJvmArgs().contains('-DpersistBuild=true')) {
                // Upload repository cache for speedup maven install 
                sh script: """
                    cp -r /jenkins-common/.kem2/repository .
                    tar czf repository.tar.gz repository
                """
                s3Upload(file: 'repository.tar.gz', bucket: 'k8s-bucket-devops', path: "ke-ci-repository/${targetBranch()}/repository.tar.gz", force: true)

                // Upload Build Result
                sh script: """
                    mkdir -p ${targetBranch()}
                    outputs=\$(find "\$(pwd)" -path '*test_data/*.zip' | sed 's/.*/&/')
                    for out in \$outputs; do
                        cp \$out ${targetBranch()}
                    done
                """
                withAWS(region: 'us-west-2', credentials: 'aws_global_s3_cp') {
                    s3Upload(bucket: 'k8s-bucket-devops', path: "ke-ci/", includePathPattern: "${targetBranch()}/*")
                }
            }
        } catch (Exception err) {
            echo err.getMessage()
        }
    }
}

def downloadArchive() {
    timestamps {
        withAWS(region: 'us-west-2', credentials: 'aws_global_s3_cp') {
            if (s3DoesObjectExist(bucket: 'k8s-bucket-devops', path: "ke-ci-repository/${targetBranch()}/repository.tar.gz")) {
                s3Download(file: 'repository.tar.gz', bucket: 'k8s-bucket-devops', path: "ke-ci-repository/${targetBranch()}/repository.tar.gz", force: true)
                sh script: """
                    tar zxf repository.tar.gz 
                    mv repository/* /jenkins-common/.kem2/repository/
                    ls -al /jenkins-common/.kem2/repository/
                """
            }
            s3Download(file: './tmp', bucket: 'k8s-bucket-devops', path: "ke-ci/${targetBranch()}/", force: true)
            sh script: """
                if [ -d ./tmp/ke-ci/${targetBranch()} ]; then
                    mv ./tmp/ke-ci/${targetBranch()} ./src/examples/test_data
                    ls -al ./src/examples/test_data
                fi
                rm -rf ./tmp
            """
            s3Download(file: 'coverage/jacococli.jar', bucket: 'k8s-bucket-devops', path: "ke-ci/jacococli.jar", force: true)
        }
    }
}

def defaultJvmArgs() {
    return params.args.isEmpty() ? '-DskipBuild=true' : params.args
}

def targetBranch() {
    try {
        return ghprbTargetBranch
    } catch (Exception err) {
        if (!params.branch.isEmpty()) {
            return params.branch
        }
        return 'newten-dev'
    }
}