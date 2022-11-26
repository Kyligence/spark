package devopslib

import io.kyligence.devopslib.Utils
import io.kyligence.devopslib.byzer.ByzerClientConfig
import io.kyligence.devopslib.byzer.ByzerClientFactory
import io.kyligence.devopslib.byzer.ByzerFacade
import spock.lang.Specification

class ByzerFacadeTest extends Specification {

    def "test basic"() {
        given:
        def config = ByzerClientConfig.builder()
                .enableHttps(true)
                .host("zen.kyligence.io")
                .account("XXXXXXXXX")
                .password("XXXXXXXXXXXX")
                .tenant("devops")
                .build()

        def client = ByzerClientFactory.create(config)

        def facade = new ByzerFacade(config, client)

        when:
        facade.executeNotebook(new File(Utils.resourcesPath() + "/notebook/ke4_cicd_etl.bznb"))

        then:
        1 == 1
    }
}
