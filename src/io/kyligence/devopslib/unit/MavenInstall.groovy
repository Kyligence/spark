package io.kyligence.devopslib.unit

import io.kyligence.devopslib.Utils

class MavenInstall extends Unit<Void, Integer> {

    final String CMD = "mvn -U clean install -T 4C -Dmaven.compile.fork=true -DskipTests"

    MavenInstall(String name) {
        super(name)
    }

    @Override
    void run(Void _) {
        Utils.ctx.sh(script: CMD)
        this.setResult(0)
    }

    @Override
    boolean isSuccess() {
        return this.getResult() != null && this.getResult() == 0
    }
}

