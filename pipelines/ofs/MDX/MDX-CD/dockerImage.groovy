pipeline {
    agent none
    stages {
       stage("version generate") {
          agent {
          kubernetes {
yaml """
spec:
  containers:
  - name: "kimdx"
    command:
    - "cat"
    image: "457798666374.dkr.ecr.us-west-2.amazonaws.com/gittools/gitversion:5.2.4-linux-centos-7-netcoreapp3.1"
    imagePullPolicy: "Always"
    resources:
      limits:
        memory: "1024Mi"
        cpu: "1000m"
      requests:
        memory: "1024Mi"
        cpu: "1000m"
    tty: true
    volumeMounts:
    - mountPath: "/jenkins-common"
      name: "volume-0"
      readOnly: false
  - name: "jnlp"
    image: "jenkins/inbound-agent:4.3-4"
    resources:
      requests:
        cpu: "100m"
        memory: "256Mi"
    volumeMounts:
    - mountPath: "/jenkins-common"
      name: "volume-0"
      readOnly: false
  volumes:
  - name: "volume-0"
    persistentVolumeClaim:
      claimName: "jenkins-common"
      readOnly: false
"""
          }
        }
            steps {
                container('kimdx') {

                  timestamps {
                      checkout([$class: "GitSCM", branches: [[name: "${branch}"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${credential}", url: "https://github.com/Kyligence/MDX-docker.git"]]])
                      script {
                          versionNumber = sh returnStdout: true , script: "/tools/dotnet-gitversion ${env.WORKSPACE} | awk -F \"[\\\"]\" '/InformationalVersion/{print\$4}'"
                          versionNumber = versionNumber.trim()
                          echo "versionNumber: $versionNumber"
                          seg = versionNumber.split("-")
                          echo "seg: $seg"
                          if (seg[1] == ''){
                              seg[1] = "${branch}" as String
                              echo "seg[1]: $seg[1]"
                              versionNumber = seg.join("-")
                          }
                          sh "mkdir -p /jenkins-common/jobs/${env.JOB_NAME}/${env.BUILD_ID}/"
                          sh "echo $versionNumber > /jenkins-common/jobs/${env.JOB_NAME}/${env.BUILD_ID}/version"
                          sh "echo -n `cat /jenkins-common/jobs/${env.JOB_NAME}/${env.BUILD_ID}/version`"
                      }
                }
              }
            }
        }
        stage("save images") {
          agent {
          kubernetes {
yaml """
spec:
  containers:
  - name: "kimdx"
    command:
    - "cat"
    image: "457798666374.dkr.ecr.us-west-2.amazonaws.com/jenkinsslave/kimdx:alpine"
    imagePullPolicy: "Always"
    resources:
      limits:
        memory: "1024Mi"
        cpu: "1000m"
      requests:
        memory: "1024Mi"
        cpu: "1000m"
    tty: true
    volumeMounts:
    - mountPath: "/jenkins-common"
      name: "volume-0"
      readOnly: false
    - mountPath: "/var/run/docker.sock"
      name: "dockersock"
  - name: "jnlp"
    image: "jenkins/inbound-agent:4.3-4"
    resources:
      requests:
        cpu: "100m"
        memory: "256Mi"
    volumeMounts:
    - mountPath: "/jenkins-common"
      name: "volume-0"
      readOnly: false
  volumes:
  - name: "volume-0"
    persistentVolumeClaim:
      claimName: "jenkins-common"
      readOnly: false
  - name: "dockersock"
    hostPath:
      path: "/var/run/docker.sock"
"""
          }
        }
            steps {
                container('kimdx') {
                  timestamps {
                    checkout([$class: "GitSCM", branches: [[name: "${branch}"]], doGenerateSubmoduleConfigurations: false, extensions: [], submoduleCfg: [], userRemoteConfigs: [[credentialsId: "${credential}", url: "https://github.com/Kyligence/MDX-docker.git"]]])
                    script {
                        final version = sh returnStdout: true , script: "echo -n `cat /jenkins-common/jobs/${env.JOB_NAME}/${env.BUILD_ID}/version`"
                        final arg_name = "MDX_VERSION"
                        final imageTag = "registry.kyligence.io/kyligence/mdx-alpine:${version}"
                        echo "version number ${version},package url null,image tag ${imageTag}"
                        dir("docker/alpine") {
                            sh "make mdx ${arg_name}=${version} PACKAGE_URL=null"
                        }
                        withDockerRegistry(credentialsId: 'registry-kyligence-io', url: 'https://registry.kyligence.io') {
                            sh "docker push ${imageTag}"
                        }
                        sh "docker save ${imageTag} -o /jenkins-common/jobs/${env.JOB_NAME}/mdx-${version}.tar.gz"
                        sh "docker rmi ${imageTag}"
                        sh "rm -rf /jenkins-common/jobs/${env.JOB_NAME}/${env.BUILD_ID}"
                      }
                  }
                }
            }
        }
    }
}