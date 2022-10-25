package io.kyligence.devopslib.jenkins

import com.fasterxml.jackson.databind.JsonNode
import okhttp3.ResponseBody
import retrofit2.Call
import retrofit2.http.GET
import retrofit2.http.Path
import retrofit2.http.Query

interface JenkinsClient {


    @GET("/blue/rest/organizations/jenkins/pipelines/{folder}/{jobFullName}/runs")
    Call<JsonNode> getJobRuns(@Path(value = "folder", encoded = true) String folder, @Path(value = "jobFullName", encoded = true) String jobFullName,
                              @Query("start") int start, @Query("limit") int limit)

    @GET("/blue/rest/organizations/jenkins/pipelines/{folder}/pipelines/{jobFullName}/runs/{runNumber}/")
    Call<JsonNode> getJobRun(@Path(value = "folder", encoded = true) String folder, @Path(value = "jobFullName", encoded = true) String jobFullName,
                             @Path("runNumber") String runNumber)

    @GET("/blue/rest/organizations/jenkins/pipelines/{folder}/pipelines/{jobFullName}/runs/{runNumber}/nodes")
    Call<JsonNode> getJobRunNodes(@Path(value = "folder", encoded = true) String folder, @Path(value = "jobFullName", encoded = true) String jobFullName,
                                  @Path("runNumber") String runNumber, @Query("limit") int limit)

    @GET("/blue/rest/organizations/jenkins/pipelines/{folder}/pipelines/{jobFullName}/runs/{runNumber}/nodes/{nodeNumber}/steps/")
    Call<JsonNode> getJobRunSteps(@Path(value = "folder", encoded = true) String folder, @Path(value = "jobFullName", encoded = true) String jobFullName,
                                  @Path("runNumber") String runNumber, @Path("nodeNumber") String nodeNumber)

    @GET("/job/{folder}/job/{jobFullName}/{runNumber}/api/json?pretty=true&tree=actions[parameters[name,value]]")
    Call<JsonNode> getJobParameters(@Path(value = "folder", encoded = true) String folder, @Path(value = "jobFullName", encoded = true) String jobFullName,
                                    @Path("runNumber") String runNumber)

    @GET("/blue/rest/organizations/jenkins/pipelines/{folder}/pipelines/{jobFullName}/runs/{runNumber}/blueTestSummary/")
    Call<JsonNode> getJobTestSummary(@Path(value = "folder", encoded = true) String folder, @Path(value = "jobFullName", encoded = true) String jobFullName,
                                     @Path("runNumber") String runNumber)

    @GET("/blue/rest/organizations/jenkins/pipelines/{folder}/pipelines/{jobFullName}/runs/{runNumber}/tests/")
    Call<JsonNode> getJobRunTests(@Path(value = "folder", encoded = true) String folder, @Path(value = "jobFullName", encoded = true) String jobFullName,
                                  @Path("runNumber") String runNumber, @Query("start") int start, @Query("limit") int limit)


    @GET("/blue/rest/organizations/jenkins/pipelines/{folder}/pipelines/{jobFullName}/runs/{runNumber}/log/?download=true")
    Call<ResponseBody> downloadJobRunLog(@Path(value = "folder", encoded = true) String folder, @Path(value = "jobFullName", encoded = true) String jobFullName,
                                         @Path("runNumber") String runNumber)

    @GET("/blue/rest/organizations/jenkins/pipelines/{folder}/pipelines/{jobFullName}/runs/{runNumber}/nodes/{nodeNumber}/steps/{stepNumber}/log/?download=true")
    Call<ResponseBody> downloadJobStepLog(@Path(value = "folder", encoded = true) String folder, @Path(value = "jobFullName", encoded = true) String jobFullName,
                                          @Path("runNumber") String runNumber, @Path("nodeNumber") String nodeNumber, @Path("stepNumber") String stepNumber)

}
