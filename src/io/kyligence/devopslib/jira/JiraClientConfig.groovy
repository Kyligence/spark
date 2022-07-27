package io.kyligence.devopslib.jira


import groovy.transform.builder.Builder

@Builder
class JiraClientConfig {
    String clientId;
    String clientSecret
    String refreshToken
    String baseUrl;
}
