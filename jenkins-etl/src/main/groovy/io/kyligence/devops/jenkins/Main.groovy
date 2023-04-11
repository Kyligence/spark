package io.kyligence.devops.jenkins

import io.kyligence.devops.jenkins.client.JenkinsClientFactory
import io.kyligence.devops.jenkins.client.JenkinsClientImpl
import io.kyligence.devops.jenkins.client.JenkinsConfig
import io.kyligence.devops.jenkins.etl.*

class Main {

    static void main(String[] args) {
        if (args[0].equals("extract")) {
            switch (args[1]) {
                case "aws-mixed":
                    extractAWSMixed()
                    break
                case "aws":
                    extractAWS()
                    break
                default:
                    throw new IllegalArgumentException()
            }
        } else if (args[0].equals("transform")) {
            switch (args[1]) {
                case "aws-mixed":
                    transformAWSMixed()
                    break
                case "aws":
                    transformAWS()
                    break
                default:
                    throw new IllegalArgumentException()
            }

        } else {
            throw new IllegalArgumentException()
        }

    }

    static void extractAWSMixed() {
        final String s3Accesskey = System.getProperty("AWS_ACCESS_KEY_ID");
        final String s3secretKey = System.getProperty("AWS_SECRET_ACCESS_KEY");
        final String s3bucket = System.getProperty("AWS_STORE_BUCKET", "devops-jenkins-history");

        final String jenkinsUsername = System.getProperty("JENKINS_USERNAME")
        final String jenkinsPassword = System.getProperty("JENKINS_PASSWORD")

        final JenkinsConfig config = new JenkinsConfig("aws-mixed", "http://jenkins.cicd:8080/", jenkinsUsername, jenkinsPassword);
        config.setS3Accesskey(s3Accesskey);
        config.setS3secretKey(s3secretKey);
        config.setS3StoreBucket(s3bucket);
        config.setCompatibleGCP(true)
        final JenkinsClientImpl client = JenkinsClientFactory.create(config);
        final JenkinsExtract extract = new JenkinsExtract(config, client);

        extract.execute("KE", "KE-CI-On-AWS_Cloud");
    }

    static void extractAWS() {
        final String s3Accesskey = System.getProperty("AWS_ACCESS_KEY_ID");
        final String s3secretKey = System.getProperty("AWS_SECRET_ACCESS_KEY");
        final String s3bucket = System.getProperty("AWS_STORE_BUCKET", "devops-jenkins-history");

        final String jenkinsUsername = System.getProperty("JENKINS_USERNAME")
        final String jenkinsPassword = System.getProperty("JENKINS_PASSWORD")

        final JenkinsConfig config = new JenkinsConfig("aws", "http://devops-jenkins:8080/", jenkinsUsername, jenkinsPassword);
        config.setS3Accesskey(s3Accesskey);
        config.setS3secretKey(s3secretKey);
        config.setS3StoreBucket(s3bucket);
        final JenkinsClientImpl client = JenkinsClientFactory.create(config);
        final JenkinsExtract extract = new JenkinsExtract(config, client);

        extract.execute("KC", "Lightning-PR-CHECK");
    }

    static void transformAWSMixed() {
        final String s3Accesskey = System.getProperty("AWS_ACCESS_KEY_ID");
        final String s3secretKey = System.getProperty("AWS_SECRET_ACCESS_KEY");
        final String s3bucket = System.getProperty("AWS_STORE_BUCKET", "devops-jenkins-history");

        final String jenkinsUsername = System.getProperty("JENKINS_USERNAME")
        final String jenkinsPassword = System.getProperty("JENKINS_PASSWORD")

        final JenkinsConfig config = new JenkinsConfig("aws-mixed", "http://jenkins.cicd:8080/", jenkinsUsername, jenkinsPassword);
        config.setS3Accesskey(s3Accesskey);
        config.setS3secretKey(s3secretKey);
        config.setS3StoreBucket(s3bucket);
        config.setCompatibleGCP(true)
        final JenkinsTransform transform = new JenkinsTransform(config, [NewtenBasicPostAnalyzer.class, ModuleTestStatePostAnalyzer.class, ModuleTestDurationPostAnalyzer.class]);

        transform.execute("KE", "KE-CI-On-AWS_Cloud");
    }

    static void transformAWS() {
        final String s3Accesskey = System.getProperty("AWS_ACCESS_KEY_ID");
        final String s3secretKey = System.getProperty("AWS_SECRET_ACCESS_KEY");
        final String s3bucket = System.getProperty("AWS_STORE_BUCKET", "devops-jenkins-history");

        final String jenkinsUsername = System.getProperty("JENKINS_USERNAME")
        final String jenkinsPassword = System.getProperty("JENKINS_PASSWORD")

        final JenkinsConfig config = new JenkinsConfig("aws", "http://devops-jenkins:8080/", jenkinsUsername, jenkinsPassword);
        config.setS3Accesskey(s3Accesskey);
        config.setS3secretKey(s3secretKey);
        config.setS3StoreBucket(s3bucket);
        final JenkinsTransform transform = new JenkinsTransform(config, [LightningBasicPostAnalyzer.class]);

        transform.execute("KC", "Lightning-PR-CHECK");
    }


}
