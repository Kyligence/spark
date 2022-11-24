package io.kyligence.devopslib.jenkins.etl


import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ObjectMetadata
import com.fasterxml.jackson.databind.node.ObjectNode
import io.kyligence.devopslib.Utils
import io.kyligence.devopslib.jenkins.JenkinsClientImpl
import io.kyligence.devopslib.jenkins.JenkinsConfig

class JenkinsExtract implements Serializable {

    private static final String ROOT_PREFIX = "original/%s"
    private static final JOB_RUNS_KEY_PREFIX = ROOT_PREFIX + "/%s/%s/%s"
    private static final STEP_LOG_KEY_PREFIX = JOB_RUNS_KEY_PREFIX + "/step_logs"


    private String storeBucket

    private JenkinsConfig config

    private JenkinsClientImpl client

    private AmazonS3 s3client

    JenkinsExtract(JenkinsConfig config, JenkinsClientImpl client) {
        this.config = config
        this.client = client
        this.storeBucket = config.getS3StoreBucket()

        s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(config.getS3Accesskey(), config.getS3secretKey())))
                .withRegion(Regions.US_WEST_2)
                .build();
    }

    void execute(String jobFolder, String jobName) {
        def jobRuns = client.getJobRuns(jobFolder, jobName, 0, 50)
        Utils.log("fetch [${jobRuns.size()}] jobs in [${jobFolder}/${jobName}]")
        for (run in jobRuns) {
            def runNumber = run.get("id").textValue()

            if (!run.get("state").textValue().equals("FINISHED")) {
                Utils.log("job [${runNumber}] unfinished, skip it.")
                continue
            }

            if (s3client.doesObjectExist(storeBucket, "${String.format(JOB_RUNS_KEY_PREFIX, config.getPlatform(), jobFolder, jobName, runNumber)}/.flag")) {
                Utils.log("job [${runNumber}] extracted, skip it.")
                continue
            }

            Utils.log("job [${runNumber}] extracting...")

            def parameters = client.getJobParameters(jobFolder, jobName, runNumber)
            ((ObjectNode) run).set("parameters", parameters)

            def nodes = client.getJobRunNodes(jobFolder, jobName, runNumber)
            for (node in nodes) {
                def nodeNumber = node.get("id").textValue()
                def steps = client.getJobRunSteps(jobFolder, jobName, runNumber, nodeNumber)
                ((ObjectNode) node).set("steps", steps)


                for (step in steps) {
                    if (step.get("result").textValue().equals("FAILURE")) {
                        def stepNumber = step.get("id").textValue()
                        def stepLogStream = client.downloadJobStepLog(jobFolder, jobName, runNumber, nodeNumber, stepNumber)

                        s3client.putObject(storeBucket, "${String.format(STEP_LOG_KEY_PREFIX, config.getPlatform(), jobFolder, jobName, runNumber)}/step_${nodeNumber}_${stepNumber}.log",
                                stepLogStream, new ObjectMetadata())
                    }
                }
            }
            ((ObjectNode) run).set("nodes", nodes)

            s3client.putObject(storeBucket, "${String.format(JOB_RUNS_KEY_PREFIX, config.getPlatform(), jobFolder, jobName, runNumber)}/run.json", run.toPrettyString())

            def testSummary = client.getJobTestSummary(jobFolder, jobName, runNumber)
            int testsTotal = testSummary.get("total").intValue()

            def testcases = null
            if (testsTotal > 0) {
                testcases = client.getJobRunTests(jobFolder, jobName, runNumber, 0, testsTotal)
            }
            ((ObjectNode) testSummary).set("testcases", testcases)


            s3client.putObject(storeBucket, "${String.format(JOB_RUNS_KEY_PREFIX, config.getPlatform(), jobFolder, jobName, runNumber)}/test.json", testSummary.toPrettyString())

            def logStream = client.downloadJobRunLog(jobFolder, jobName, runNumber)
            s3client.putObject(storeBucket, "${String.format(JOB_RUNS_KEY_PREFIX, config.getPlatform(), jobFolder, jobName, runNumber)}/console.log", logStream, new ObjectMetadata())

            s3client.putObject(storeBucket, "${String.format(JOB_RUNS_KEY_PREFIX, config.getPlatform(), jobFolder, jobName, runNumber)}/.flag", "DONE")

            Utils.log("job [${runNumber}] extract done!")
        }

    }
}
