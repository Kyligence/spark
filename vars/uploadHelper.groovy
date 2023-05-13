def call(String ctl, String file_name, String remote_path, String bucket, String dir_name = 'sourcecode', String region='', String credentials=null) {
    println "uploadHelper -> ctl[${ctl}],file_name[${file_name}],remote_path[${remote_path},bucket[${bucket},dir_name[${dir_name},region[${region},credentials[${credentials}]"
    switch (ctl) {
        case 'AWSS3':
            uploadAwsS3(file_name, remote_path, bucket, region, credentials, dir_name)
            break
        case 'SERVER':
            uplaodServer(file_name, remote_path, bucket, dir_name, credentials)
            break
        case 'NEXUS':
            uploadNexus(file_name, remote_path, bucket, dir_name)
            break
    }
}

// 上传制品包到Aws S3存储
def uploadAwsS3(file_name, remote_path, bucket, region, credentials, dir_name = 'sourcecode') {
    println "uploadAwsS3 -> file_name[${file_name}],remote_path[${remote_path}],bucket[${bucket}],dir_name[${dir_name}],region[${region}],credentials[${credentials}]"
    dir(dir_name) {
        timestamps {
            script {
                withAWS(region: region, credentials: credentials) {
                    s3Upload(bucket: bucket, path: remote_path, file: file_name)
                }
            }
        }
    }
}

// 上传包到Azure服务器
def uplaodServer(file_name, remote_path, remote_ip, dir_name = 'sourcecode', credentials='azure-4xuser') {
    println "uplaodServer -> file_name[${file_name}],remote_path[${remote_path}],remote_ip[${remote_ip}],dir_name[${dir_name}],credentials[${credentials}]"
    dir(dir_name) {
        timestamps {
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
                sshPut remote: remote, from: file_name, into: remote_path
                sshCommand remote: remote, command: """
                    ls -lh ${remote_path}
                """
            }
        }
    }
}

// 上传制品包到Nexus制品库
def uploadNexus(file_name, package_path, repo, dir_name = 'sourcecode', credentials='nexus-raw') {
    print "uploadNexus -> file_name[${file_name}],package_path[${package_path}],repo[${repo}],dir_name[${dir_name}],credentials[${credentials}]"
    dir(dir_name) {
        timestamps {
            script {
                // 上传制品库
                withCredentials([usernamePassword(credentialsId: credentials, passwordVariable: 'passwd', usernameVariable: 'user')]) {
                    pushRawArtifactsByApi(repo, package_path, file_name, file_name, user, passwd)
                }
            }
        }
    }
}
