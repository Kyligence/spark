def call() {
    def ut1 = testForModules(
            'UTest-Stage-1', [
            "src/core-common",
            "src/core-job",
            "src/core-storage",
            "src/core-metadata",
            "src/yinglong-enterprise-core-metadata",
            "src/query",
            "src/smart",
            "src/source-hive",
            "src/streaming",
            "src/server-base",
            "src/server",
            "src/second-storage/clickhouse",
            "src/second-storage/core",
            "src/second-storage/core-ui"], [])
    def ut2 = testForModules(
            'UTest-Stage-2', [
            "src/spark-project/engine-spark",
            "src/spark-project/kylin-user-session",
            "src/spark-project/kylin-user-session-dep",
            "src/spark-project/source-jdbc",
            "src/spark-project/sparder",
            "src/spark-project/spark-common",
            "src/spark-project/spark-it",
            "src/tool",
            "src/datasource-sdk",
            "src/external-catalog/external-catalog-sdk",
            "src/assembly",
            "src/second-storage/clickhouse-it"], [])
    def it1 = testForModules('ITest-Stage-1', ["src/kap-it"], ["!io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest#testAllQueries"])
    def it2 = testForModules('ITest-Stage-2', ["src/kap-it"], ["io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest#testAllQueries"])

    def allTestStages = ut1 + ut2 + it1 + it2

    echo 'parallel run all tests...'
    parallel allTestStages
}

def testForModules(String stage, List<String> modules, List<String> tests) {
    return [(stage): {
        // 'container': use the same container to execute in parallel in the current pod
        // 'node(POD_LABEL)': multiple pod in parallel, and this situation needs to rely on external shared storage
        container('maven') {
            def willTestModules = modules.join(",")
            def willTestCases = tests.join(",")
            sh script: """
                if [ ! -d ${stage} ]; then
                    mkdir ${stage} && cp -arf ./sourcecode/* ./${stage}/
                fi

                cd ./${stage} && mvn clean test --fail-at-end \
                -pl ${willTestModules} \
                -Dtest='${willTestCases}' \
                -DfailIfNoTests=false \
                -Duser.timezone=GMT+8
            """
        }
    }]
}
