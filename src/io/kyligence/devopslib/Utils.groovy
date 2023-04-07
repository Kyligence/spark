package io.kyligence.devopslib

import groovy.transform.SourceURI

import java.nio.file.Paths

class Utils implements Serializable {

    @SourceURI
    static URI internalUri

    static def ctx

    static Properties loadJobEnv(String name, String platform) {
        def jobEnv = new Properties()
        def envFile = Paths.get(resourcesPath(), "envs/${name}/${platform}.env").toFile()
        jobEnv.load(new FileReader(envFile))
        log("loading job env from ${envFile}")
        log("${jobEnv}")
        return jobEnv
    }

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
