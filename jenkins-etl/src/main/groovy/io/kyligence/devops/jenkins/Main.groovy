package io.kyligence.devops.jenkins

import io.kyligence.devops.jenkins.client.JenkinsClientFactory
import io.kyligence.devops.jenkins.client.JenkinsClientImpl
import io.kyligence.devops.jenkins.client.JenkinsConfig
import io.kyligence.devops.jenkins.etl.JenkinsExtract

class Main {

    static void main(String[] args) {
        final String s3Accesskey = System.getProperty("AWS_ACCESS_KEY_ID");
        final String s3secretKey = System.getProperty("AWS_SECRET_ACCESS_KEY");
        final String s3bucket = System.getProperty("AWS_STORE_BUCKET", "devops-jenkins-history");

        final JenkinsConfig config = new JenkinsConfig("gcp", "http://cicd-gcp.kyligence.com/", "dev", "kylin@2022");
        config.setS3Accesskey(s3Accesskey);
        config.setS3secretKey(s3secretKey);
        config.setS3StoreBucket(s3bucket);
        final JenkinsClientImpl client = JenkinsClientFactory.create(config);
        final JenkinsExtract extract = new JenkinsExtract(config, client);

        extract.execute("KE4", "Newten_CI_On_GCP");
        extract.execute("Devops", "check_pipeline");
    }
}
