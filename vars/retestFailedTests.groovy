import hudson.tasks.junit.TestResultAction

@NonCPS
def call() {
    def testResultActions = currentBuild.rawBuild.getActions(TestResultAction.class)
    if (testResultActions.isEmpty()) {
        echo 'Test result not found...'
        return
    }

    def lastTestResultAction = testResultActions.last()
    if (lastTestResultAction.getFailedTests().isEmpty()) {
        echo 'No Failed Tests...'
        return
    }

    echo "${lastTestResultAction.getFailCount()} failed tests will retesting..."
    echo "${lastTestResultAction.getFailedTests().collect({ "${it.className}.${it.testName}" }).join('\n')}"

    def willRetestCases = lastTestResultAction.getFailedTests().collect({ "${it.className}#${it.testName}" }).join(',')

    container('maven') {
        def testStage = 'RTest-Stage'
        sh script: """
            if [ ! -d ${testStage} ]; then
                mkdir ${testStage} && cp -arf ./sourcecode/* ./${testStage}/
            fi

            cd ./${testStage} && mvn clean test --fail-at-end \
            -Dtest='${willRetestCases}' \
            -DfailIfNoTests=false \
            -Duser.timezone=GMT+8
            
            # always return zero in retest
            exit 0
        """
        junit skipPublishingChecks: true, testResults: "${testStage}/**/target/surefire-reports/*.xml"
    }
}