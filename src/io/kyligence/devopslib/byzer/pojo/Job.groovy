package io.kyligence.devopslib.byzer.pojo

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.node.ArrayNode
import groovy.transform.ToString

@JsonIgnoreProperties(ignoreUnknown = true)
class Job {

    @Override
    public String toString() {
        return "Job{" +
                "job_id='" + job_id + '\'' +
                ", job_name='" + job_name + '\'' +
                ", content='" + content + '\'' +
                ", duration=" + duration +
                ", status=" + status +
                ", result='" + result + '\'' +
                '}';
    }

    static class Status {
        static int RUNNING = 0;
        static int SUCCESS = 1;
        static int FAILED = 2;
        static int KILLED = 3;
        static int RETRYING = 4;
        static int NOT_EXIST = -1;
    }

    String job_id
    String job_name
    String content
    int duration
    int status
    String result

    boolean isDone() {
        return this.status in [Status.FAILED, Status.SUCCESS, Status.KILLED]
    }



}
