import java.util.concurrent.LinkedBlockingQueue
import java.util.Collections

def call() {
    container('maven') {
        def modules = sh(script: '''
                cd sourcecode
                mvn help:evaluate -Dexpression=project.modules | grep -v "^\\[" | grep -v "<\\/*strings>" | sed 's/<\\/*string>//g' | sed 's/[[:space:]]//'
            ''', returnStdout: true).trim().split("\n").collect({ it.trim() }).findAll { it.startsWith("src") || it.startsWith("kyligence/src") || it == "kylin" }
        println "origin modules: ${modules}"

        def kap_it_module = modules.findAll({ it.contains('src/kap-it') }).getAt(0)
        if (kap_it_module) {
            modules.removeAll([kap_it_module])
            modules.addAll([
                    "${kap_it_module} -Dtest='!io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest'",
                    "${kap_it_module} -Dtest='io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest'"
            ])
        }

        Collections.reverse(modules)
        println "final modules: ${modules}"

        def taskQueue = modules as LinkedBlockingQueue

        def worker1 = createWorker("UTest-Stage-1", taskQueue)
        def worker2 = createWorker("UTest-Stage-2", taskQueue)
        def worker3 = createWorker("UTest-Stage-3", taskQueue)
        def worker4 = createWorker("UTest-Stage-4", taskQueue)

        def allTestStages = worker1 + worker2 + worker3 + worker4
        parallel allTestStages
    }
}

def createWorker(String stage, LinkedBlockingQueue taskQueue) {
    def jvmArgs = defaultJvmArgs()
    return [(stage): {
        container('maven') {
            script {
                sh script: """
                    if [ ! -d ${stage} ]; then
                        mkdir ${stage} && cp -arf ./sourcecode/* ./${stage}/
                    fi
                """
                def item = taskQueue.poll()
                while (item != null) {
                    retry(2){
                        println "retry [${_count}], jvmArgs: ${jvmArgs}, stage: ${stage}, item: ${item}"
                        def _count = 0
                        try {
                            sh script: """
                                cd ./${stage}
                                mvn clean test --fail-at-end \
                                -pl ${item} \
                                -DfailIfNoTests=false \
                                -Duser.timezone=GMT+8 ${jvmArgs}
                            """
                            item = taskQueue.poll()
                        } catch(ex) {
                            echo "Exception: ${ex.toString()}"
                            echo "ooops - caught: ${ex.class}"
                            echo "ooops - with msg: ${ex.message}"
                            echo "ooops - backtrace: ${ex.stackTrace}"
                            jvmArgs = ''
                            _count++
                        } 
                    }
                }
            }
        }
    }]
}

def defaultJvmArgs() {
    return params.args.isEmpty() ? '-DskipBuild=true' : params.args
}