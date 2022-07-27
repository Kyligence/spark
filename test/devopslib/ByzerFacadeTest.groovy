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
                .account("cheng.zuo@kyligence.io")
                .password("2ada15a9d5b5b219de8cfa4d52fa0bcddc8e41731f94b2a21434d32270160f3259bf1b1fdc7ab803355acc5bd53b53fcf0fd31fa64d2101a13b296e657a1111eda95c8ebe965d52e7a5a4dd55c4c50a0ebc12c45edc5bb96193e71f511dacc81db3a2b21f1dc4ba61a6af35e57b83fa600cbbc711c0d590de1fb92a9512344d4849ba0841c4fa7c25145e7ade2f0f0edd0d1a9006edb296544121390d6d017e3c816bdf4b2c3da33e83f84947da484d11717107eadaef9b09dd76d3de5d4e0221269ab9fed4f0b8fe9fa6d73fe5914330a6a8bf2d8caef29c58e52d39731cabca865ecca5749b9da883319cde1c2557297b2b0a636447fd6c64cc2816ad1caf1")
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
