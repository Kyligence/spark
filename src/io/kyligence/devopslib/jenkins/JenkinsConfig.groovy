package io.kyligence.devopslib.jenkins

class JenkinsConfig {

    JenkinsConfig(String platform, String baseUrl, String username, String password) {
        this.platform = platform
        this.baseUrl = baseUrl
        this.username = username
        this.password = password
    }

    String platform

    String baseUrl

    String username

    String password

    String s3StoreBucket

    String s3Accesskey

    String s3secretKey

}
