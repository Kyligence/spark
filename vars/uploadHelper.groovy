def call(ctl,file_name,remote_path,region,credentialsId,isCloned=true) {
     switch(ctl) {
        case 'AwsS3':
            uploadAwsS3(region, credentials, bucket, path, ga_name)
        break
        case 'SERVER':
            uplaodRemoteServer(subject,to,body,from)
        break
        case 'NEXUS':
            uploadNexus(subject,to,body,from)
        break
    }
}

// 上传制品包到Aws S3存储
def uploadAwsS3(region, credentials, bucket, path, ga_name) {
    dir('sourcecode') {
        timestamps {
            println("uploadAwsS3 -> path: ${path}, ga_name: ${ga_name}")
            withAWS(region: region, credentials: credentials) {
                s3Upload(bucket: bucket, path: path, file: ga_name)
            }
        }
    }
}

// 上传包到Azure服务器
def uplaodRemoteServer(file_name,remote_ip,remote_path) {
    script {
        def remote = [:]
        remote.name = 'azure_test_env'
        withCredentials([usernamePassword(credentialsId: 'azure-4xuser',
                usernameVariable: 'REMOTE_USERNAME',
                passwordVariable: 'REMOTE_PASSWORD')]) {
            remote.user = REMOTE_USERNAME
            remote.password = REMOTE_PASSWORD
        }
        remote.host = remote_ip
        remote.allowAnyHosts = true
        dir('sourcecode') {
            sshPut remote: remote, from: file_name, into: remote_path
        }
        sshCommand remote: remote, command: """
            ls -lh ${remote_path}
        """
    }
}

// 上传制品包到Nexus制品库
def uploadNexus(repo, package_path, file_name) {
    timestamps {
        container('build') {
            dir('sourcecode') {
                script {
                    println("repo: ${repo}, package_path: ${package_path}, file_name: ${file_name}")
                    // 上传制品库
                    withCredentials([usernamePassword(credentialsId: 'nexus-raw', passwordVariable: 'passwd', usernameVariable: 'user')]) {
                        pushRawArtifactsByApi(repo, package_path, file_name, file_name, user, passwd)
                    }
                }
            }
        }
    }
}