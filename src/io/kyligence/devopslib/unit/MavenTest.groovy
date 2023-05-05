package io.kyligence.devopslib.unit


import io.kyligence.devopslib.Utils

class MavenTest extends Unit<String, Integer> {

    MavenTest(String name) {
        super(name)
    }

    @Override
    void run(String jvmArgs) {
        Utils.ctx.sh(script: """
                    mvn clean test --fail-at-end \
                    -pl ${getName()} \
                    -DfailIfNoTests=false \
                    -Duser.timezone=GMT+8 ${jvmArgs}
                    -Djava.net.preferIPv4Stack=true
                """)
        this.setResult(0)

    }

    @Override
    boolean isSuccess() {
        return this.getResult() != null && this.getResult() == 0
    }
}

