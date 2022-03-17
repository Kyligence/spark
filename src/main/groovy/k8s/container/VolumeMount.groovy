package io.kyligence.devopslib.k8s.container

class VolumeMount {

    String name, subPath, mountPath

    VolumeMount(String name, String subPath, String mountPath) {
        this.name = name
        this.subPath = subPath
        this.mountPath = mountPath
    }
}
