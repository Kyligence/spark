import java.util.concurrent.LinkedBlockingQueue

def call() {
    Map<String, List<String>> testModules = evalTestModules()
    println("test modules: ${testModules}")
    def taskWorkers = [:]

    def keys = testModules.keySet()
    for (key in keys) {
        def taskQueue = new LinkedBlockingQueue(testModules.get(key))
        if (taskQueue.size() < 1) {
            continue
        }

        def workerCount = (taskQueue.size() / 10) >= 4 ? 4 : (taskQueue.size() / 10 + 1)
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
    def allSubModules = []

    container('maven') {
        dir("sourcecode") {
            def parentModules = ["", "kylin"]
            for (String parentModule in parentModules) {
                def childModules = sh(script: """
                    if [ ! -z ${parentModule} ]; then
                        cd ${parentModule}
                    fi
                    mvn help:evaluate -Dexpression=project.modules | grep -v "^\\[" | grep -v "<\\/*strings>" | sed 's/<\\/*string>//g' | sed 's/[[:space:]]//'
                """, returnStdout: true)
                        .trim()
                        .split("\n")
                        .findAll({ !it.startsWith("Downloaded from") && !it.startsWith("Downloading from") && !it.startsWith("Downloadingfrom") && !it.startsWith("Progress") })
                        .collect({ parentModule.isEmpty() ? it.trim() : "${parentModule}/${it.trim()}" })

                println "child modules: ${childModules}"
                allSubModules.addAll(childModules)
            }

            allSubModules.removeAll(parentModules)
            println "all submodules: ${allSubModules}"

            def kapItModules = []
            def kapIt = allSubModules.findAll({ it.contains('src/kap-it') }).getAt(0)
            if (kapIt) {
                // remove kap-it modules
                allSubModules.removeAll([kapIt])

                // expand kap-it modules
                kapItModules.addAll([
                        "${kapIt} -Dtest='!io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest'",
                        "${kapIt} -Dtest='io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest'"
                ])
            }

            // remove clickhouse-it modules
            def clickhouse_it_module = allSubModules.findAll({ it.contains('src/second-storage') })
            if (clickhouse_it_module) {
                println "clickhouse-it modules: ${clickhouse_it_module}"
                allSubModules.removeAll(clickhouse_it_module)
            }

            Collections.reverse(allSubModules)
            return ["UTest": allSubModules, "KapIT": kapItModules]
        }

    }
}

def createWorker(String workerName, LinkedBlockingQueue taskQueue) {
    return [(workerName): {
        podTemplate(yaml: readTrusted('pipelines/gcp/KE4/Newten CI On GCP/jenkins-agent.yaml')) {
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
