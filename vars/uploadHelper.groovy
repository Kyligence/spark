// def call(ctl, file_name, remote_path, bucket, region='', credentials=null) {
//     switch (ctl) {
//         case 'AwsS3':
//             uploadAwsS3(file_name, remote_path, bucket, region, credentials)
//             break
//         case 'SERVER':
//             uplaodRemoteServer(file_name, remote_path, bucket, credentials)
//             break
//         case 'NEXUS':
//             uploadNexus(file_name, remote_path, bucket)
//             break
//     }
// }

// 上传制品包到Aws S3存储
def uploadAwsS3(file_name, remote_path, bucket, region, credentials) {
    dir('sourcecode') {
        timestamps {
            println("uploadAwsS3 ->  file_name: ${file_name}, remote_path: ${remote_path}, bucket: ${bucket}, region: ${region}, credentials: ${credentials}")
            withAWS(region: region, credentials: credentials) {
                s3Upload(bucket: bucket, path: remote_path, file: file_name)
            }
        }
    }
}

// 上传包到Azure服务器
def uplaodRemoteServer(file_name, remote_path, remote_ip, credentials='azure-4xuser') {
    script {
        def remote = [:]
        remote.name = 'azure_test_env'
        withCredentials([usernamePassword(credentialsId: credentials,
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
def uploadNexus(file_name, package_path, repo, credentials='nexus-raw') {
    timestamps {
        container('build') {
            dir('sourcecode') {
                script {
                    println("repo: ${repo}, package_path: ${package_path}, file_name: ${file_name}")
                    // 上传制品库
                    withCredentials([usernamePassword(credentialsId: credentials, passwordVariable: 'passwd', usernameVariable: 'user')]) {
                        pushRawArtifactsByApi(repo, package_path, file_name, file_name, user, passwd)
                    }
                }
            }
        }
    }
}
