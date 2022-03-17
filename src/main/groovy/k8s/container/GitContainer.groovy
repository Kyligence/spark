package io.kyligence.devopslib.k8s.container

class GitContainer extends Container {

    GitContainer() {
        this.name = 'git'
        this.image = 'bitnami/git:latest'
        this.tty = true
        this.command = ['cat']
        this.volumeMounts = [new VolumeMount('jenkins-common', null, '/var/jenkins_common')]
    }
}
