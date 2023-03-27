import org.kohsuke.github.GitHub

import java.util.concurrent.LinkedBlockingQueue


def call() {
    final int MAX_UTEST_WORKERS = 2
    final int MAX_ITEST_WORKERS = 3

    Map<String, List<String>> testModules = evalTestModules()
    println("test modules: ${testModules}")
    def taskWorkers = [:]

    def keys = testModules.keySet()
    for (key in keys) {
        def taskQueue = new LinkedBlockingQueue(testModules.get(key))
        if (taskQueue.size() < 1) {
            continue
        }

        def workerCount = { if (key == 'UTest') MAX_UTEST_WORKERS else MAX_ITEST_WORKERS }()

        if (taskQueue.size() < workerCount) {
             workerCount = 1
        }

        for (int i = 1; i <= workerCount; i++) {
            taskWorkers += createWorker("${key}-Stage-${i}", taskQueue)
        }
    }

    stage("PreProcess Sourcecode") {
        preprocessSourcecode()
    }

    if (params.failFast) {
        taskWorkers.failFast = true
    }

    println("task workers: ${taskWorkers}")

    stage("Testing") {
        parallel taskWorkers
    }
}

def preprocessSourcecode() {
    container('maven') {
        sh script: """
            mkdir -p /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/repository              

            cd /root/.m2/repository && find ./io/kyligence -exec cp -arf --parents {} /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/repository/ \\;
            du -h -d2 /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/repository/io/kyligence

            cd /root/.m2/repository && find ./org/apache/kylin -exec cp -arf --parents {} /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/repository/ \\;
            du -h -d2 /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/repository/org/apache/kylin
        """

        if (params.skipBuild) {
            def testWhichBranch = params.branch ?: ghprbTargetBranch
            withAWS(region: 'us-west-2', credentials: 'aws_global_s3') {
                s3Download(file: "./test_data_cache", bucket: 'k8s-bucket-devops', path: "ke-ci/${testWhichBranch}/", force: true)
                sh script: """
                if [ -d ./test_data_cache/ke-ci/${testWhichBranch} ]; then
                    mv ./test_data_cache/ke-ci/${testWhichBranch} ./sourcecode/kylin/src/examples/test_data
                    ls -al ./sourcecode/kylin/src/examples/test_data
                fi
            """
            }
        }

        sh script: """
            mkdir -p /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/sourcecode && cp -arf ./sourcecode/* /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/sourcecode
            du -h -d2 /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/sourcecode
        """
    }
}


Map<String, List<String>> evalTestModules() {
    List<String> allSubModules = []

    container('maven') {
        dir("sourcecode") {
            def parentModules = ["", "kylin"]


            def changeFiles = []
            withCredentials([usernamePassword(credentialsId: 'kyligence-git', passwordVariable: 'token', usernameVariable: 'login')]) {
                def github = GitHub.connect(login, token)
                def repo = github.getRepository("Kyligence/KAP")
                def pr = repo.getPullRequest(Integer.valueOf(params.ghprbPullId))

                def changeFilesReq = pr.listFiles().iterator()
                while (changeFilesReq.hasNext()) {
                    def file = changeFilesReq.next()
                    if (file.getStatus() in ["added", "removed", "modified", "renamed"]) {
                        changeFiles.add(file.getFilename())
                    }
                }
            }

            changeFiles = changeFiles.stream().distinct().collect()
            println("change files: ${changeFiles}")

//             def changeModules = sh(script: "java -jar /tools/mat-0.1.2.jar -b -r -lm -d `pwd` -cl ${changeFiles.join(",")} | grep -A 2 'build command:'", returnStdout: true)
//                     .trim()
//                     .split("\n")
//                     .last()
//                     .split(",")
//                     .collect({ it.trim() })
//             println("change modules: ${changeModules}")
//             allSubModules.addAll(changeModules)

            allSubModules.removeAll(parentModules)
            println "all submodules: ${allSubModules}"

            // remove clickhouse-it modules
            def clickhouse_it_module = allSubModules.findAll({ it.contains('src/second-storage') })
            if (clickhouse_it_module) {
                println "clickhouse-it modules: ${clickhouse_it_module}"
                allSubModules.removeAll(clickhouse_it_module)
            }

            // get slow modules
            def slowModules = slowModules(allSubModules)
            println("slow modules: ${slowModules}")

            Collections.reverse(allSubModules)

            return ["UTest": allSubModules, "ITest": slowModules]
        }

    }
}

List<String> slowModules(List<String> allModules) {

    // 特别注意： slow modules 中的模块顺序最好是按照从大到小排列，这样能保证消费时间相对均匀
    List<String> slowModules = []

    // kylin-it 一般需要 40 mins
    def kylinIt = allModules.findAll({ it.contains('src/kylin-it') }).getAt(0)
    if (kylinIt) {
        // remove kylin-it modules
        allModules.removeAll([kylinIt])

        slowModules.addAll([kylinIt])
    }

    // 分别是 30+ 和 20+ mins
    def kapIt = allModules.findAll({ it.contains('src/kap-it') }).getAt(0)
    if (kapIt) {
        // remove kap-it modules
        allModules.removeAll([kapIt])

        // expand kap-it modules
        slowModules.addAll(["${kapIt} -Dtest='!io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest'"])
        slowModules.addAll(["${kapIt} -Dtest='io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest'"])
    }

    // 一般是 20 mins
    def sparkIt = allModules.findAll({ it.contains('spark-it') }).getAt(0)
    if (sparkIt) {
        // remove spark-it modules in all modules list
        allModules.removeAll([sparkIt])

        slowModules.addAll([sparkIt])
    }

    return slowModules
}

def createWorker(String workerName, LinkedBlockingQueue taskQueue) {
    return [(workerName): {
        def workerAgent = workerName.startsWith('UTest') ? "worker-low-agent.yaml" : "worker-high-agent.yaml"

        podTemplate(yaml: readTrusted("pipelines/gcp/KE4/Newten CI On GCP/${workerAgent}")) {
            node(POD_LABEL) {
                container('maven') {
                    script {
                        preprocessTestData(workerName)
                        println("worker[${workerName}], total task[${taskQueue.size()}]")
                        runTest(workerName, taskQueue)
                    }
                }
            }
        }
    }]
}

def preprocessTestData(String target) {
    sh script: """
        if [ ! -d ${target} ]; then
            ls /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/sourcecode
            mkdir ${target} && cp -arf /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/sourcecode/* ./${target}/
            du -h -d2 ./${target}/
        fi

        cp -arf /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/repository/io/kyligence/* /root/.m2/repository/io/kyligence
        cp -arf /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/repository/org/apache/kylin/* /root/.m2/repository/org/apache/kylin
    """
}

def runTest(String workerName, LinkedBlockingQueue taskQueue) {
    def jvmArgs = params.skipBuild ? "-DskipBuild=true" : ""

    if (workerName.startsWith("UTest")) {
        jvmArgs = "${jvmArgs} -DargLine='-Xms2G -Xmx5G'"
    }

    def task = taskQueue.poll()
    def retry = 0
    try {
        while (task != null) {
            println "retry [${retry}], jvmArgs: ${jvmArgs}, stage: ${workerName}, task: ${task}, [${taskQueue.size()}] tasks left"
            try {
                sh script: """
                    cd ./${workerName}
                    mvn clean test --fail-at-end \
                    -pl ${task} \
                    -DfailIfNoTests=false \
                    -Duser.timezone=GMT+8 ${jvmArgs}
                """
                task = taskQueue.poll()
            } catch (ex) {
                if (params.failFast && ex instanceof org.jenkinsci.plugins.workflow.steps.FlowInterruptedException) {
                    println "failfast..."
                    throw ex
                }

                println "Exception: ${ex.toString()}\n \
                        ooops - caught: ${ex.class}\n \
                        ooops - with msg: ${ex.message}\n \
                        ooops - backtrace: ${ex.stackTrace}"

                jvmArgs = ''
                retry += 1
                if (retry > 1) {
                    if (params.failFast) {
                        error 'Retry twice and still fail'
                    } else {
                        task = taskQueue.poll()
                    }
                }
            }
        }
    } finally {
        collectTestResults()
        collectTestCoverage()
    }
}

def collectTestResults() {
    sh """
        mkdir -p /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/agg_test_results
        find . -path '*-Stage-*surefire-reports*xml' -exec cp -arf --parents {} /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/agg_test_results/ \\;
        du -h -d1 /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/agg_test_results/
    """
}

def collectTestCoverage() {
    sh """
        mkdir -p /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/agg_test_coverages
        find . -path '*-Stage-*target*jacoco.exec' -exec cp -arf --parents {} /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/agg_test_coverages/ \\;
        du -h -d1 /var/jenkins_tmp/${JOB_NAME}/${BUILD_NUMBER}/agg_test_coverages/
    """
}
