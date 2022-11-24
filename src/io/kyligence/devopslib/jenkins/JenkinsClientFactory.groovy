package io.kyligence.devopslib.jenkins

import com.cloudbees.groovy.cps.NonCPS

import okhttp3.Cookie
import okhttp3.FormBody
import okhttp3.HttpUrl
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory

import io.kyligence.devopslib.Utils

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

class JenkinsClientFactory implements Serializable {

    private final static String SET_COOKIE_URL = "/login"
    private final static String LOGIN_URL = "/j_spring_security_check"

    private JenkinsClientFactory() {
    }

    static class CookieJar implements okhttp3.CookieJar {

        Map<String, Cookie> cookies = new HashMap<>()

        @Override
        void saveFromResponse(HttpUrl url, List<Cookie> cookies) {
            cookies.each {
                this.cookies.put(it.name(), it)
            }
        }

        @Override
        List<Cookie> loadForRequest(HttpUrl url) {
            return new ArrayList<Cookie>(cookies.values())
        }
    }

    @NonCPS
    static JenkinsClientImpl create(JenkinsConfig config) {
        def cookieAuthenticator = new CookieJar()
        final OkHttpClient client = new OkHttpClient.Builder()
                .readTimeout(60, TimeUnit.SECONDS)
                .connectTimeout(60, TimeUnit.SECONDS)
                .cookieJar(cookieAuthenticator)
                .build();

        final Response setCookie = client.newCall(new Request.Builder()
                .url(Paths.get(config.baseUrl, SET_COOKIE_URL).toString())
                .build())
                .execute()

        if (!setCookie.isSuccessful()) {
            Utils.log("set cookie failed[${setCookie.code()}]: ${setCookie.body().string()}")
            throw new RuntimeException()
        }

        def loginForm = new FormBody.Builder()
                .add("j_username", config.username)
                .add("j_password", config.password)
                .build()

        def loginResponse = client.newCall(new Request.Builder()
                .url(Paths.get(config.baseUrl, LOGIN_URL).toString())
                .header("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36")
                .post(loginForm)
                .build())
                .execute()

        if (!loginResponse.isSuccessful()) {
            Utils.log("login failed[${loginResponse.code()}]: ${loginResponse.body().string()}")
            throw new RuntimeException()
        }

        final Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(config.baseUrl)
                .callFactory(client)
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        return new JenkinsClientImpl(retrofit.create(JenkinsClient.class))
    }


}
