package devopslib

import io.kyligence.devopslib.jenkins.JenkinsClientFactory
import io.kyligence.devopslib.jenkins.JenkinsConfig
import io.kyligence.devopslib.jenkins.etl.JenkinsExtract
import spock.lang.Specification

class JenkinsExtractTest extends Specification {

    def "test"() {
        given:
        def config = new JenkinsConfig("aws", "https://devopsjenkins.kyligence.io/", "zuoc", "KYLIN@2020")
        config.setS3Accesskey("AKIAWVFXNJCDA76HVQSY")
        config.setS3secretKey("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")
        config.setS3StoreBucket("devops-jenkins-history")

        def client = JenkinsClientFactory.create(config)
        def extract = new JenkinsExtract(config, client)

        def folder = "KE4"
        def jobFullName = "KE-CI-On_Cloud"

        when:
        extract.execute(folder, jobFullName)

        then:
        1 == 1
    }
}
