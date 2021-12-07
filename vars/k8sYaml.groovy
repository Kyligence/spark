@Grab('org.codehaus.groovy:groovy-yaml:3.0.5')
import groovy.yaml.YamlBuilder
import io.kyligence.devopslib.k8s.container.Container

@NonCPS
def call(List<Container> containers) {
    def yaml = new YamlBuilder()
    yaml([kind: 'Pod',
          name: 'jenkins-agent',
          spec: [containers: containers,
                 volumes   : [[
                                      name                 : 'jenkins-common',
                                      persistentVolumeClaim: [
                                              claimName: 'jenkins-common'
                                      ]
                              ]]]
    ])
    yaml.toString()
}


