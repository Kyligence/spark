package io.kyligence.devopslib

import groovy.transform.SourceURI

import java.nio.file.Paths

class Utils implements Serializable {

    @SourceURI
    static URI internalUri

    static def ctx

    static String absoluteRootPath() {
        return Paths.get(internalUri).toString().split("src")[0]
    }

    static String resourcesPath() {
        return absoluteRootPath() + "resources"
    }

    static void log(String msg) {
        if (System.getenv("JENKINS_HOME") && ctx) {
            ctx.echo msg
        } else {
            println msg
        }
    }

}
