package io.kyligence.devops.jenkins.etl

import io.kyligence.devops.jenkins.client.JenkinsConfig
import spock.lang.Specification

class JenkinsTransformTest extends Specification {

    def "test"() {
        given:
        def config = new JenkinsConfig("gcp", "http:/DFSDFDASDFSADF.com/", "XXXXX", "XXXXXXX2")
        config.setS3Accesskey("SDFSFAFD")
        config.setS3secretKey("SDFASDFSADF/ASDFASDFAS")
        config.setS3StoreBucket("devops-jenkins-history")

        def transform = new JenkinsTransform(config)

        when:
        transform.execute("KE4", "Newten_CI_On_GCP");

        then:
        1 == 1

    }
}
