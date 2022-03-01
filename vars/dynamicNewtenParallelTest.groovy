import java.util.concurrent.LinkedBlockingQueue

def call() {
    container('maven') {
        def modules = sh(script:'''
                cd sourcecode
                mvn help:evaluate -Dexpression=project.modules | grep -v "^\\[" | grep -v "<\\/*strings>" | sed 's/<\\/*string>//g' | sed 's/[[:space:]]//'
            ''', returnStdout: true).trim().split("\n").collect({ it.trim() }).findAll { it.startsWith("src") }
        def blacklistModules = ['src/kap-it', 'src/server-base', 'src/spark-project/engine-spark']
        def taskQueue = [
            "src/kap-it -Dtest='!io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest'",
            "src/kap-it -Dtest='io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest'",
            "src/server-base",
            "src/spark-project/engine-spark"] as LinkedBlockingQueue
        modules.each { m ->
            if (!blacklistModules.contains(m)) {
                taskQueue.add(m)
            }
        }

        def worker1 = createWorker("UTest-Stage-1", taskQueue)
        def worker2 = createWorker("UTest-Stage-2", taskQueue)
        def worker3 = createWorker("UTest-Stage-3", taskQueue)
        def worker4 = createWorker("UTest-Stage-4", taskQueue)

        def allTestStages = worker1 + worker2 + worker3 + worker4
        parallel allTestStages
    }
}

def createWorker(String stage, LinkedBlockingQueue taskQueue) {
    return [(stage): {
        container('maven') {
            script {
                sh script: """
                    if [ ! -d ${stage} ]; then
                        mkdir ${stage} && cp -arf ./sourcecode/* ./${stage}/
                    fi
                """
                def item = taskQueue.poll()
                while(item != null) {
                    sh script: """
                        cd ./${stage}
                        mvn clean test --fail-at-end \
                        -pl ${item} \
                        -DfailIfNoTests=false \
                        -Duser.timezone=GMT+8 ${defaultJvmArgs()}
                    """
                    item = taskQueue.poll()
                }
            }
        }
    }]
}

def defaultJvmArgs() {
    return params.args.isEmpty() ? '-DskipBuild=true' : params.args
}