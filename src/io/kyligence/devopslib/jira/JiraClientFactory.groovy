package io.kyligence.devopslib.jira

import groovy.json.JsonBuilder
import okhttp3.MediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import retrofit2.Retrofit

class JiraClientFactory {

    private final static String ACCESS_TOKEN_URL = "https://auth.atlassian.com/oauth/token"

    private JiraClientFactory() {
    }

    static JiraClient create(JiraClientConfig config) {
        final OkHttpClient client = new OkHttpClient()

        JsonBuilder oauthData = new JsonBuilder()
        oauthData {
            grant_type "refresh_token"
            client_id config.clientId
            client_secret config.clientSecret
            refresh_token config.refreshToken
        }

        def oauthBody = RequestBody.create(MediaType.get("application/json"), oauthData.toString())
        def oauthRequest = new Request.Builder()
                .url(ACCESS_TOKEN_URL)
                .post(oauthBody)
                .build()

        try (def oauthResponse = client.newCall(oauthRequest).execute()) {
            if (!oauthResponse.isSuccessful()) {
                throw new RuntimeException()
            }
            final Retrofit retrofit = new Retrofit.Builder()
                    .baseUrl(config.baseUrl)
                    .callFactory(client)
                    .build();
            return retrofit.create(JiraClient.class);
        }
    }
}
