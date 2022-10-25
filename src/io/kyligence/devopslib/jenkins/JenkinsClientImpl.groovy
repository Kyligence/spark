package io.kyligence.devopslib.jenkins

import com.fasterxml.jackson.databind.JsonNode
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import retrofit2.Call

class JenkinsClientImpl {

    private static final Configuration JSONPATH_CONF = Configuration.builder().jsonProvider(new JacksonJsonNodeJsonProvider())
            .options(Option.ALWAYS_RETURN_LIST, Option.SUPPRESS_EXCEPTIONS).build()

    JenkinsClient client

    JenkinsClientImpl(JenkinsClient client) {
        this.client = client
    }

    JsonNode getJobRuns(String folder, String jobFullName, int start, int limit) {
        return execute(client.getJobRuns(folder, jobFullName, start, limit))
    }

    JsonNode getJobRun(String folder, String jobFullName, String runNumber) {
        return execute(client.getJobRun(folder, jobFullName, runNumber))
    }

    JsonNode getJobRunNodes(String folder, String jobFullName, String runNumber) {
        return execute(client.getJobRunNodes(folder, jobFullName, runNumber, 1000))
    }

    JsonNode getJobRunSteps(String folder, String jobFullName, String runNumber, String nodeNumber) {
        return execute(client.getJobRunSteps(folder, jobFullName, runNumber, nodeNumber))
    }

    JsonNode getJobParameters(String folder, String jobFullName, String runNumber) {
        def data = execute(client.getJobParameters(folder, jobFullName, runNumber))
        return JsonPath.using(JSONPATH_CONF).parse(data).read("\$..parameters[*]")
    }

    JsonNode getJobTestSummary(String folder, String jobFullName, String runNumber) {
        return execute(client.getJobTestSummary(folder, jobFullName, runNumber))
    }

    JsonNode getJobRunTests(String folder, String jobFullName, String runNumber, int start, int limit) {
        return execute(client.getJobRunTests(folder, jobFullName, runNumber, start, limit))
    }

    InputStream downloadJobRunLog(String folder, String jobFullName, String runNumber) {
        def content = execute(client.downloadJobRunLog(folder, jobFullName, runNumber))
        return content.byteStream()

    }

    InputStream downloadJobStepLog(String folder, String jobFullName, String runNumber, String nodeNumber, String stepNumber) {
        def content = execute(client.downloadJobStepLog(folder, jobFullName, runNumber, nodeNumber, stepNumber))
        return content.byteStream()
    }


    private <T> T execute(Call<T> caller) {
        def resp = caller.execute()
        if (!resp.isSuccessful()) {
            println("------ execute failed ------")
            println(caller.request().url())
            println(resp.code())
            println(resp.errorBody().string())
            println("----------------------------")
            throw new RuntimeException(resp.message())
        }

        return resp.body();
    }
}
