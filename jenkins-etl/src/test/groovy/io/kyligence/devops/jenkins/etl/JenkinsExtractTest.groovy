package io.kyligence.devops.jenkins.etl


import io.kyligence.devops.jenkins.client.JenkinsClientFactory
import io.kyligence.devops.jenkins.client.JenkinsConfig
import spock.lang.Specification

class JenkinsExtractTest extends Specification {

    def "test"() {
        given:
        def config = new JenkinsConfig("gcp", "http://cicd-gcp.kyligence.com/", "dev", "kylin@2022")
        config.setS3Accesskey("AKIAWVFXNJCDA76HVQSY")
        config.setS3secretKey("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")
        config.setS3StoreBucket("devops-jenkins-history")

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
