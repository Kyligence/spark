package io.kyligence.devopslib.byzer

import io.kyligence.devopslib.Utils
import io.kyligence.devopslib.byzer.pojo.ExecuteScript
import io.kyligence.devopslib.byzer.pojo.Job
import okhttp3.MediaType
import okhttp3.MultipartBody
import okhttp3.RequestBody

class ByzerFacade implements Serializable {

    private ByzerClientConfig config

    private ByzerClient client

    ByzerFacade(ByzerClientConfig config, ByzerClient client) {
        this.config = config
        this.client = client
    }

    void executeNotebook(File notebookFile) {
        def importCall = client.importNotebook(MultipartBody.Part.createFormData("file", notebookFile.getName(), RequestBody.create(MediaType.get("multipart/form-data"), notebookFile)))
        def importResp = importCall.execute()
        if (!importResp.isSuccessful()) {
            Utils.log(importResp.errorBody().string())
            throw new IllegalStateException("import notebook error.")
        }

        if (importResp.body().data.size() != 1) {
            throw new IllegalStateException()
        }

        def notebookId = importResp.body().data.get(0).id
        def notebook = client.getNotebook(notebookId).execute().body().data

        Utils.log "imported notebook ${notebookFile.name} done, import name is ${notebook.name}. execute it now"

        def i = 1
        notebook.cell_list.each {
            Utils.log "${i++}. running cell [${it.id}]"
            executeScript(ExecuteScript.builder().notebook(notebook.getName()).cell_id(it.getId()).sql(it.getContent()).build())
        }

        Utils.log "excuted notebook ${notebook.name} done"

        client.deleteNotebook(notebook.id).execute()
        Utils.log "deleted notebook ${notebook.name} done"
    }

    void executeScript(ExecuteScript script) {
        def jobId = client.executeScript(script).execute().body().data
        def job = waitingForJobDone(jobId.job_id)
        if (job.status != Job.Status.SUCCESS) {
            throw new IllegalStateException("notebook ${script.notebook} executed failed. sql:\n ${script.sql}")
        }
    }

    Job waitingForJobDone(String jobId) {
        def job = client.getJob(jobId).execute().body().data
        def timeout = 0
        while (!job.isDone()) {
            if (timeout > 30 * 60 * 1000) {
                throw new IllegalStateException("wait timeout!")
            }

            if (!(timeout % (60 * 1000))) {
                Utils.log "   waiting for job ${jobId} done...[${timeout / (60 * 1000)} mins]"
            }

            sleep(1000)
            timeout += 1000

            job = client.getJob(jobId).execute().body().data
        }

        return job
    }
}
