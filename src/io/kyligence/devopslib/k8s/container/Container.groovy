package io.kyligence.devopslib.k8s.container

class Container {

    String name
    String image
    boolean tty
    List<String> command
    List<VolumeMount> volumeMounts
}
