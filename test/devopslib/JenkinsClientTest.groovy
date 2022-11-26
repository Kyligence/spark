package devopslib

import com.fasterxml.jackson.databind.node.ObjectNode
import io.kyligence.devopslib.jenkins.JenkinsClientFactory
import io.kyligence.devopslib.jenkins.JenkinsConfig
import org.apache.commons.io.FileUtils
import spock.lang.Specification

class JenkinsClientTest extends Specification {

    def "test basic"() {
        given:
        def config = new JenkinsConfig("aws", "hXXXXXXXXX", "XXXXXXX", "XXXXXXXX")

        when:
        def folder = "KE4"
        def jobFullName = "KE-CI-On_Cloud"

        def jobDir = new File(jobFullName)
        if (!jobDir.exists()) {
            jobDir.mkdirs()
        }

        def client = JenkinsClientFactory.create(config)

        def jobRuns = client.getJobRuns(folder, jobFullName, 0, 10)
        for (run in jobRuns) {
            def runNumber = run.get("id").textValue()

            def runDir = new File(runNumber, jobDir)
            if (runDir.exists()) {
                runDir.deleteDir()
            }
            runDir.mkdir()

            def parameters = client.getJobParameters(folder, jobFullName, runNumber)
            ((ObjectNode) run).set("parameters", parameters)

            def stepLogDir = new File("step_logs", runDir)
            stepLogDir.mkdir()

            def nodes = client.getJobRunNodes(folder, jobFullName, runNumber)
            for (node in nodes) {
                def nodeNumber = node.get("id").textValue()
                def steps = client.getJobRunSteps(folder, jobFullName, runNumber, nodeNumber)
                ((ObjectNode) node).set("steps", steps)


                for (step in steps) {
                    if (step.get("result").textValue().equals("FAILURE")) {
                        def stepNumber = step.get("id").textValue()
                        def stepLogStream = client.downloadJobStepLog(folder, jobFullName, runNumber, nodeNumber, stepNumber)
                        FileUtils.copyInputStreamToFile(stepLogStream, new File("step_${nodeNumber}_${stepNumber}.log", stepLogDir))
                    }
                }
            }
            ((ObjectNode) run).set("nodes", nodes)

            new File("run.json", runDir).withWriter("utf-8") {
                writer -> writer.write("[${run.toString()}]")
            }

            def testSummary = client.getJobTestSummary(folder, jobFullName, runNumber)
            int testsTotal = testSummary.get("total").intValue()

            def testcases = null
            if (testsTotal > 0) {
                testcases = client.getJobRunTests(folder, jobFullName, runNumber, 0, testsTotal)
            }
            ((ObjectNode) testSummary).set("testcases", testcases)


            new File("test.json", runDir).withWriter("utf-8") {
                writer -> writer.write("[${testSummary.toString()}]")
            }

            def logStream = client.downloadJobRunLog(folder, jobFullName, runNumber)
            FileUtils.copyInputStreamToFile(logStream, new File("console.log", runDir))

        }

        then:
        1 == 1

    }
}
