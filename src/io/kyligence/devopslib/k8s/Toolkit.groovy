package io.kyligence.devopslib.k8s

import groovy.text.GStringTemplateEngine
import io.kyligence.devopslib.Utils

import java.nio.file.Paths

static def loadYaml(String path, Map<String, String> jobEnv) {
    def yamlFile = Paths.get(Utils.resourcesPath(), path).toFile()
    Utils.log("loading yaml from ${yamlFile}")

    def engine = new GStringTemplateEngine()
    def template = engine.createTemplate(yamlFile.text).make(jobEnv)

    return template.toString()
}
