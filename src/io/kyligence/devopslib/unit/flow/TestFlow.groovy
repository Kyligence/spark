package io.kyligence.devopslib.unit.flow

import io.kyligence.devopslib.Utils
import io.kyligence.devopslib.unit.MavenTest
import org.jenkinsci.plugins.workflow.steps.FlowInterruptedException

import java.util.concurrent.LinkedBlockingQueue

class TestFlow {

    final String name;

    final int totalSize

    private final LinkedBlockingQueue taskQueue

    TestFlow(String name, LinkedBlockingQueue taskQueue) {
        this.name = name
        this.taskQueue = taskQueue
        this.totalSize = taskQueue.size()
    }

    void execute(boolean skipBuild, boolean failFast) {
        def jvmArgs = skipBuild ? "-DskipBuild=true" : ""

        if (this.name.startsWith("UTest")) {
            jvmArgs = "${jvmArgs} -DargLine='-Xms2G -Xmx5G'"
        }

        String task = taskQueue.poll()
        def retry = 0

        Utils.ctx.dir(this.name) {
            Utils.ctx.sh("cp -arf ../sourcecode/* ./")
        }

        while (task != null) {
            Utils.log "retry [${retry}], jvmArgs: ${jvmArgs}, stage: ${this.name}, task: ${task}, total [${this.totalSize}] and [${taskQueue.size()}] tasks left"
            try {
                def mavenTest = new MavenTest(task)

                Utils.ctx.dir(this.name) {
                    mavenTest.run(jvmArgs)
                }

                if (!mavenTest.isSuccess()) {
                    throw new RuntimeException("test [${task}] returned exit code ${mavenTest.getResult()}")
                }

                task = taskQueue.poll()
            } catch (ex) {
                if (failFast && ex instanceof FlowInterruptedException) {
                    Utils.log "failfast..."
                    throw ex
                }

                Utils.log "Exception: ${ex.toString()}\n \
                        ooops - caught: ${ex.class}\n \
                        ooops - with msg: ${ex.message}\n \
                        ooops - backtrace: ${ex.stackTrace}"

                jvmArgs = ''
                retry += 1
                if (retry > 1) {
                    if (failFast) {
                        Utils.ctx.error 'Retry twice and still fail'
                    } else {
                        task = taskQueue.poll()
                    }
                }
            }
        }
    }

}
