package devopslib

import okhttp3.MediaType
import okhttp3.MultipartBody
import okhttp3.RequestBody
import spock.lang.Specification

class ByzerClientTest extends Specification {

    def "test basic"() {
        given:
        def config = ByzerClientConfig.builder()
                .enableHttps(true)
                .host("zen.kyligence.io")
                .account("SDFSDFSDF")
                .password("DDDFDFSDF")
                .tenant("devops")
                .build()

        when:
        def client = ByzerClientFactory.create(config)

        then:
        client != null

        when:
        def call = client.executeScript(ExecuteScript.builder().sql("!show version;").build())

        then:
        call != null

        when:
        def resp = call.execute()

        then:
        resp.isSuccessful()
        println(resp.body().data)

        when:
        def jobCall = client.getJob(resp.body().data.job_id)
        def jobResp = jobCall.execute();

        then:
        jobResp.isSuccessful()
        println(jobResp.body().data)

        when:
        def file = RequestBody.create(MediaType.get("multipart/form-data"), new File("/Users/cheng.zuo/Documents/devopslib/resources/notebook/ci_report.bznb"))
        def importCall = client.importNotebook(MultipartBody.Part.createFormData("file", "ci_report.bznb", file))
        def importResp = importCall.execute()

        then:
        importResp.isSuccessful()
        println(importResp.body().data)

        when:
        def notebookCall = client.getNotebook("564")
        def notebookResp = notebookCall.execute()

        then:
        notebookResp.isSuccessful()
        println(notebookResp.body().data)

    }
}
