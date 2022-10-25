package io.kyligence.devopslib.jenkins

import okhttp3.*
import retrofit2.Retrofit
import retrofit2.converter.jackson.JacksonConverterFactory

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

    static JenkinsClientImpl create(JenkinsConfig config) {
        def cookieAuthenticator = new CookieJar()
        final OkHttpClient client = new OkHttpClient.Builder()
                .readTimeout(60, TimeUnit.SECONDS)
                .connectTimeout(60, TimeUnit.SECONDS)
                .cookieJar(cookieAuthenticator)
                .build();

        def setCookie = client.newCall(new Request.Builder()
                .url(Paths.get(config.baseUrl, SET_COOKIE_URL).toString())
                .build())
                .execute()
        if (!setCookie.isSuccessful()) {
            println("set cookie failed[${setCookie.code()}]: ${setCookie.body().string()}")
            throw new RuntimeException()
        }

        def loginForm = new FormBody.Builder()
                .add("j_username", config.username)
                .add("j_password", config.password)
                .build()

        def loginResponse = client.newCall(new Request.Builder()
                .url(Paths.get(config.baseUrl, LOGIN_URL).toString())
                .header("user-agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36")
                .header("referer", "https://devopsjenkins.kyligence.io/login?from=%2F")
                .header("origin", "https://devopsjenkins.kyligence.io")
                .header("cookie", "screenResolution=1792x1120; JSESSIONID.cab436e8=node05clq82mug0sbgbzxt7l3zbpw43787.node0")
                .post(loginForm)
                .build())
                .execute()

        if (!loginResponse.isSuccessful()) {
            println("login failed[${loginResponse.code()}]: ${loginResponse.body().string()}")
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
