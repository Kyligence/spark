package devopslib

import io.kyligence.devopslib.jira.JiraClientConfig
import io.kyligence.devopslib.jira.JiraClientFactory
import spock.lang.Specification

class JiraClientTest extends Specification {


    def "test basic"() {
        given:
        def config = JiraClientConfig.builder()
                .baseUrl("http://127.0.0.1")
                .clientId("vO33nmOxEVIYQHwm5dDxNZn4PMG0Vm8N")
                .clientSecret("SL5TWDWki23y-O6CqCPUbuOthB56C85XQZZ40olX_eEBOTdTieg0pSvTmKG-gRaT")
                .refreshToken("v1.MfvblVJsXrbwbOnAn4mF-8KX-6hXFPO1h3L0b4FjDObDwVT0cFFWpxlMJn8kxZyOzos81k6t3NN38hKBNTIjqqE")
                .build()

        when:
        def client = JiraClientFactory.create(config)

        then:
        client != null
    }
}
