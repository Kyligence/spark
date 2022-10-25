package io.kyligence.devopslib.jenkins

class JenkinsConfig {

    JenkinsConfig(String baseUrl, String username, String password) {
        this.baseUrl = baseUrl
        this.username = username
        this.password = password
    }
    String baseUrl

    String username

    String password

}
