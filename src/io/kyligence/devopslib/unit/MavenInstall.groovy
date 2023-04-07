package io.kyligence.devopslib.unit

import io.kyligence.devopslib.Utils

class MavenInstall extends Unit<Void, Integer> {

    final String CMD = "mvn -U clean install -T 4C -Dmaven.compile.fork=true -DskipTests"

    MavenInstall(String name) {
        super(name)
    }

    @Override
    void run(Void _) {
        this.setResult(Utils.ctx.sh(script: CMD, returnStatus: true))
    }

    @Override
    boolean isSuccess() {
        return this.getResult() != null && this.getResult() == 0
    }
}

