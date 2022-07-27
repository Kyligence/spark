package io.kyligence.devopslib.byzer

import io.kyligence.devopslib.byzer.pojo.ByzerResponse
import io.kyligence.devopslib.byzer.pojo.ExecuteScript
import io.kyligence.devopslib.byzer.pojo.ImportedNotebook
import io.kyligence.devopslib.byzer.pojo.Job
import io.kyligence.devopslib.byzer.pojo.JobId
import io.kyligence.devopslib.byzer.pojo.Notebook
import okhttp3.MultipartBody
import retrofit2.Call
import retrofit2.http.Body
import retrofit2.http.DELETE
import retrofit2.http.GET
import retrofit2.http.Multipart
import retrofit2.http.POST
import retrofit2.http.Part
import retrofit2.http.Path

interface ByzerClient {

    @POST("/api/script/execution")
    Call<ByzerResponse<JobId>> executeScript(@Body ExecuteScript script);

    @GET("/api/job/{id}")
    Call<ByzerResponse<Job>> getJob(@Path("id") String id)

    @Multipart
    @POST("/api/file/import")
    Call<ByzerResponse<List<ImportedNotebook>>> importNotebook(@Part MultipartBody.Part file)

    @GET("/api/notebook/{id}")
    Call<ByzerResponse<Notebook>> getNotebook(@Path("id") String id)

    @DELETE("/api/file/{id}?type=notebook")
    Call<ByzerResponse<Object>> deleteNotebook(@Path("id") String id)
}
