package io.kyligence.devops.jenkins.etl


import io.kyligence.devops.jenkins.client.JenkinsClientFactory
import io.kyligence.devops.jenkins.client.JenkinsConfig
import spock.lang.Specification

class JenkinsExtractTest extends Specification {

    def "test"() {
        given:
        def config = new JenkinsConfig("gcp", "http://cSDFASFSADFe.com/", "XXXXXXX", "XXXXXX")
        config.setS3Accesskey("SFSAFASF")
        config.setS3secretKey("hwM2C4zVfX/SADFASDFASDF")
        config.setS3StoreBucket("devASDFASDFSFSDF-history")

        def client = JenkinsClientFactory.create(config)
        def extract = new JenkinsExtract(config, client)

        def folder = "KE4"
        def jobFullName = "Newten_CI_On_GCP"

        when:
        extract.execute("Devops", "check_pipeline");

        then:
        1 == 1
    }
}
