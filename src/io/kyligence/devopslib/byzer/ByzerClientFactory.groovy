package io.kyligence.devopslib.byzer

import com.cloudbees.groovy.cps.NonCPS
import groovy.json.JsonBuilder
import okhttp3.Cookie
import okhttp3.HttpUrl
import okhttp3.MediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

class ByzerClientFactory implements Serializable {

    private final static String ACCESS_TOKEN_API_PATH = "/api/v1/login"

    private ByzerClientFactory() {
    }

    static class CookieJar implements okhttp3.CookieJar {

        Map<String, Cookie> cookies = new HashMap<>()

        @NonCPS
        @Override
        void saveFromResponse(HttpUrl url, List<Cookie> cookies) {
            cookies.each {
                this.cookies.put(it.name(), it)
            }
        }

        @NonCPS
        @Override
        List<Cookie> loadForRequest(HttpUrl url) {
            return new ArrayList<Cookie>(cookies.values())
        }
    }

    @NonCPS
    static ByzerClient create(ByzerClientConfig config) {
        def cookieAuthenticator = new CookieJar()
        final OkHttpClient client = new OkHttpClient.Builder()
                .readTimeout(60, TimeUnit.SECONDS)
                .connectTimeout(60, TimeUnit.SECONDS)
                .cookieJar(cookieAuthenticator)
                .build();

        JsonBuilder loginData = new JsonBuilder()
        buildLoginData(loginData, config)

        def loginBody = RequestBody.create(MediaType.get("application/json; charset=utf-8"), loginData.toString())
        def loginRequest = new Request.Builder()
                .url(Paths.get(config.baseUrl(), ACCESS_TOKEN_API_PATH).toString())
                .post(loginBody)
                .build()

        def loginResponse = client.newCall(loginRequest).execute()
        if (!loginResponse.isSuccessful()) {
            println("login failed: ${loginResponse.body().string()}")
            throw new RuntimeException()
        }

        final Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(config.baseUrl())
                .callFactory(client)
                .addConverterFactory(JacksonConverterFactory.create())
                .build();
        return retrofit.create(ByzerClient.class);
    }

    @NonCPS
    static void buildLoginData(JsonBuilder loginData, ByzerClientConfig config) {
        loginData {
            account config.account
            password config.password
            tenant_name config.tenant
        }
    }

}
