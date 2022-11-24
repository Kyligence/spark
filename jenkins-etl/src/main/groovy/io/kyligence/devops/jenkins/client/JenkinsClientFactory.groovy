package io.kyligence.devops.jenkins.client

import groovy.util.logging.Slf4j
import okhttp3.*
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory

import java.nio.file.Paths
import java.util.concurrent.TimeUnit

@Slf4j
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


    static JenkinsClientImpl create(JenkinsConfig config) {
        def cookieAuthenticator = new CookieJar()
        final OkHttpClient client = new OkHttpClient.Builder()
                .readTimeout(60, TimeUnit.SECONDS)
                .connectTimeout(60, TimeUnit.SECONDS)
                .cookieJar(cookieAuthenticator)
                .build();

        def setCookieReq = new Request.Builder()
                .url(Paths.get(config.baseUrl, SET_COOKIE_URL).toString())
                .build()
        final Response setCookie = client.newCall(setCookieReq)
                .execute()

        if (!setCookie.isSuccessful()) {
            log.info("set cookie failed[${setCookie.code()}]: ${setCookie.body().string()}")
            throw new RuntimeException()
        }

        setCookie.close()

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
            log.info("login failed[${loginResponse.code()}]: ${loginResponse.body().string()}")
            throw new RuntimeException()
        }

        loginResponse.close()

        final Retrofit retrofit = new Retrofit.Builder()
                .baseUrl(config.baseUrl)
                .callFactory(client)
                .addConverterFactory(JacksonConverterFactory.create())
                .build();

        return new JenkinsClientImpl(retrofit.create(JenkinsClient.class))
    }


}
