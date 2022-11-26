package devopslib

import io.kyligence.devopslib.jira.JiraClientConfig
import io.kyligence.devopslib.jira.JiraClientFactory
import spock.lang.Specification

class JiraClientTest extends Specification {


    def "test basic"() {
        given:
        def config = JiraClientConfig.builder()
                .baseUrl("http://127.0.0.1")
                .clientId("DFSDFSDF")
                .clientSecret("SL5TSDFASDFASDFASDFASF-gRaT")
                .refreshToken("v1.SSDFSDFSDF")
                .build()

        when:
        def client = JiraClientFactory.create(config)

        then:
        client != null
    }
}
