import hudson.tasks.junit.TestResultAction

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
    echo "${collectFailedTests(lastTestResultAction, ".", "\n")}"

    def willRetestCases = collectFailedTests(lastTestResultAction)

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

@NonCPS
def collectFailedTests(TestResultAction testResultAction, String combinator = "#", String joiner = ",") {
    testResultAction.getFailedTests().collect({ "${it.className}${combinator}${it.testName}" }).join(joiner)
}