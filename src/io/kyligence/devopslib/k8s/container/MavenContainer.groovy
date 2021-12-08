package io.kyligence.devopslib.k8s.container

class MavenContainer extends Container {

    MavenContainer(String name) {
        this.name = name
        this.image = 'maven:3.8.3-openjdk-8'
        this.tty = true
        this.command = ['cat']
        this.volumeMounts = [
                new VolumeMount('jenkins-common', 'maven3', '/root/.m2'),
                new VolumeMount('jenkins-common', 'troubleshooting', '/var/troubleshooting')
        ]
    }

}
