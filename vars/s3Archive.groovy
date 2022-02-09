def call(String type) {
    if (type == 'upload') {
        uploadArchive()
    } else if (type == 'download') {
        downloadArchive()
    }
}

def uploadArchive() {
    withAWS(region: 'us-west-2', credentials: 'aws_global_s3_cp') {
        sh script: """
            mkdir -p ${BUILD_NUMBER}/report
            outputs=\$(find "\$(pwd)" -path '*surefire-reports/*.xml' | sed 's/.*/&/')
            for out in \$outputs; do
                cp \$out ${BUILD_NUMBER}/
            done
            tar czf ${BUILD_NUMBER}.tar.gz ${BUILD_NUMBER}/
        """
        s3Upload(bucket: 'k8s-bucket-devops', path: "ke-ci-result/${JOB_NAME}/", includePathPattern: "${BUILD_NUMBER}.tar.gz")
    }
    if (defaultJvmArgs().contains('-DpersistBuild=true')) {
        sh script: """
            mkdir -p ${defaultTargetBranch()}
            outputs=\$(find "\$(pwd)" -path '*test_data/*.zip' | sed 's/.*/&/')
            for out in \$outputs; do
                cp \$out ${defaultTargetBranch()}
            done
        """
        withAWS(region: 'us-west-2', credentials: 'aws_global_s3_cp') {
            s3Upload(bucket: 'k8s-bucket-devops', path: "ke-ci/", includePathPattern: "${defaultTargetBranch()}/*")
        }
    }
}

def downloadArchive() {
    timestamps {
        withAWS(region: 'us-west-2', credentials: 'aws_global_s3_cp') {
            s3Download(file: './repository.tar.gz', bucket: 'k8s-bucket-devops', path: "ke-ci-repository/repository.tar.gz", force: true)
            sh script: """
                tar zxf repository.tar.gz 
                mv repository/* /jenkins-common/.kem2/repository/
            """
            s3Download(file: './tmp', bucket: 'k8s-bucket-devops', path: "ke-ci/${defaultTargetBranch()}/", force: true)
            sh script: """
                mv ./tmp/ke-ci/${defaultTargetBranch()} ./src/examples/test_data
                rm -rf ./tmp
            """
        }
    }
}

def defaultJvmArgs() {
    return params.args.isEmpty() ? '-DskipBuild=true' : params.args
}

def defaultTargetBranch() {
    if (binding.variables.containsKey('ghprbTargetBranch')) {
        return ghprbTargetBranch
    }
    return 'newten-dev'
}