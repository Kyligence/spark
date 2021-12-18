def call() {
    def ut1 = testForModules(
            'UTest-Stage-1', [
            "src/core-common",
            "src/core-job",
            "src/core-storage",
            "src/core-metadata"], [])
    def ut2 = testForModules(
            'UTest-Stage-2', [
            "src/query",
            "src/smart",
            "src/streaming",
            "src/second-storage/clickhouse",
            "src/second-storage/core",
            "src/second-storage/core-ui"], [])
    def ut3 = testForModules(
            'UTest-Stage-3', [
            "src/spark-project/engine-spark",
            "src/spark-project/sparder",
            "src/spark-project/spark-common",
            "src/spark-project/spark-it",
            "src/tool",
            "src/external",
            "src/external-catalog",
            "src/assembly",
            "src/udf"], [])
    def ut4 = testForModules(
            'UTest-Stage-4', [
            "src/server-base",
            "src/server"], [])
    def it1 = testForModules('ITest-Stage-1', ["src/kap-it"], ["!io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest#testAllQueries"])
    def it2 = testForModules('ITest-Stage-2', ["src/kap-it"], ["io.kyligence.kap.newten.auto.NAutoBuildAndQueryTest#testAllQueries"])

    def allTestStages = ut1 + ut2 + ut3 + ut4 + it1 + it2

    echo 'parallel run all tests...'
    parallel allTestStages
}

def testForModules(String stage, List<String> modules, List<String> tests) {
    return [(stage): {
        // 'container': use the same container to execute in parallel in the current pod
        // 'node(POD_LABEL)': multiple pod in parallel, and this situation needs to rely on external shared storage
        container(stage.toLowerCase()) {
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
