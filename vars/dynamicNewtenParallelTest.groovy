import java.util.concurrent.LinkedBlockingQueue
import java.util.Collections

def call() {
    container('maven') {
        def modules = sh(script: '''
                cd sourcecode
                mvn help:evaluate -Dexpression=project.modules | grep -v "^\\[" | grep -v "<\\/*strings>" | sed 's/<\\/*string>//g' | sed 's/[[:space:]]//'
            ''', returnStdout: true).trim().split("\n").collect({ it.trim() }).findAll { it.startsWith("src") || it.startsWith("kyligence/src") || it.startsWith("kylin/src") || it == "kylin" }
        println "origin modules: ${modules}"

        def kap_it_module = modules.findAll({ it.contains('src/kap-it') }).getAt(0)
        if (kap_it_module) {
            modules.removeAll([kap_it_module])
            modules.addAll([
                "${kap_it_module} -Dtest='!io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest'",
                "${kap_it_module} -Dtest='io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest'"
            ])
        }

        def clickhouse_it_module = modules.findAll({ it.contains('src/second-storage/clickhouse-it')}).getAt(0)
        if(clickhouse_it_module) {
            modules.removeAll([clickhouse_it_module])
            modules.addAll([
                "${clickhouse_it_module} -Dtest='!io.kyligence.kap.secondstorage.tdvt.TDVTTest'",
                "${clickhouse_it_module} -Dtest='io.kyligence.kap.secondstorage.tdvt.TDVTTest'"
            ])
        }

        if ('kylin' in modules) {
            def kylin_modules = sh(script: '''
                cd sourcecode/kylin
                mvn help:evaluate -Dexpression=project.modules | grep -v "^\\[" | grep -v "<\\/*strings>" | sed 's/<\\/*string>//g' | sed 's/[[:space:]]//'
            ''', returnStdout: true).trim().split("\n").collect({ 'kylin/' + it.trim() }).findAll { it.startsWith("kylin/src") }
            println "kylin sub modules: ${kylin_modules}"

            modules.removeAll(['kylin'])
            modules.addAll(kylin_modules)
        }

        Collections.reverse(modules)
        println "final modules: ${modules}"

        def taskQueue = modules as LinkedBlockingQueue

        def worker1 = createWorker("UTest-Stage-1", taskQueue)
        def worker2 = createWorker("UTest-Stage-2", taskQueue)
        def worker3 = createWorker("UTest-Stage-3", taskQueue)
        def worker4 = createWorker("UTest-Stage-4", taskQueue)

        def allTestStages = worker1 + worker2 + worker3 + worker4
        allTestStages.failFast = true
        parallel allTestStages
    }
}

def spliteUt(List modules, String moduleName, String methordName) {
    def split_module = modules.findAll({ it.contains(moduleName)}).getAt(0)
    if(split_module) {
        modules.removeAll([split_module])
        modules.addAll([
            "${split_module} -Dtest='!${methordName}'",
            "${split_module} -Dtest='${methordName}'"
        ])
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
                def _count = 0
                while (item != null) {
                    println "retry [${_count}], jvmArgs: ${jvmArgs}, stage: ${stage}, item: ${item}"
                    try {
                        sh script: """
                            cd ./${stage}
                            mvn clean test --fail-at-end \
                            -pl ${item} \
                            -DfailIfNoTests=false \
                            -Duser.timezone=GMT+8 ${jvmArgs}
                        """
                        item = taskQueue.poll()
                    } catch (ex) {
                        echo """Exception: ${ex.toString()}\n \
                                ooops - caught: ${ex.class}\n \
                                ooops - with msg: ${ex.message}\n \
                                ooops - backtrace: ${ex.stackTrace}"""
                        jvmArgs = ''
                        _count += 1
                    }

                    if (_count == 2) {
                        error 'Retry twice and still fail'
                    }
                }
            }
        }
    }]
}

def defaultJvmArgs() {
    return params.args.isEmpty() ? '-DskipBuild=true' : params.args
}