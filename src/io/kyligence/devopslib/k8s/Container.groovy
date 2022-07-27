package io.kyligence.devopslib.container.k8s

class Container {

    String name
    String image
    boolean tty
    List<String> command
    List<VolumeMount> volumeMounts
}
