package io.kyligence.devopslib.container.k8s

class VolumeMount {

    String name, subPath, mountPath

    VolumeMount(String name, String subPath, String mountPath) {
        this.name = name
        this.subPath = subPath
        this.mountPath = mountPath
    }
}
