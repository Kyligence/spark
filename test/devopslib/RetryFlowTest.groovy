package devopslib

import io.kyligence.devopslib.unit.GithubCheckout
import io.kyligence.devopslib.unit.MavenInstall
import io.kyligence.devopslib.unit.flow.RetryFlow
import spock.lang.Specification

class RetryFlowTest extends Specification {

    def "test basic"() {
        given:

        def mavenInstall = new MavenInstall("test maven install", "/Users/cheng.zuo/Documents/newten")
        def retryFlow = new RetryFlow(mavenInstall)

        when:
        def r = retryFlow.execute(null, false, 2, 1000)

        then:
        r == 0
    }

    def "test checkout"(){

        when:
        def p =  GithubCheckout.Parameter.builder().build()

        then:
        p.org == "Kyligene"

    }
}
