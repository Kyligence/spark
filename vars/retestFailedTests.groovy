import hudson.tasks.junit.TestResultAction

def call() {
    if (skipRetest()) {
        return
    }

    echo "Will retest failed tests: "
    echo "${collectFailedTests(".", "\n")}"

    container('maven') {
        def willRetestCases = collectFailedTests()
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
def boolean skipRetest() {
    def testResultActions = currentBuild.rawBuild.getActions(TestResultAction.class)
    if (testResultActions.isEmpty()) {
        echo 'Test result not found...'
        return true
    }

    def lastTestResultAction = testResultActions.last()
    if (lastTestResultAction.getFailedTests().isEmpty()) {
        echo 'No Failed Tests...'
        return true
    }

    return false

}

@NonCPS
def collectFailedTests(String combinator = "#", String joiner = ",") {
    def lastTestResultAction = currentBuild.rawBuild.getActions(TestResultAction.class).last()
    return lastTestResultAction.getFailedTests().collect({ "${it.className}${combinator}${it.testName}" }).join(joiner)
}