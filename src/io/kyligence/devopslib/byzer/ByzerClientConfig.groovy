package io.kyligence.devopslib.byzer

import com.cloudbees.groovy.cps.NonCPS
import okhttp3.HttpUrl

class ByzerClientConfig implements Serializable {
    String account

    // zen 前端加密后的密码
    String password
    String tenant

    String host
    int port

    boolean enableHttps

    ByzerClientConfig() {
    }

    ByzerClientConfig(String account, String password, String tenant, String host, int port, boolean enableHttps) {
        this.account = account
        this.password = password
        this.tenant = tenant
        this.host = host
        this.port = port
        this.enableHttps = enableHttps
    }

    @NonCPS
    String baseUrl() {
        if (enableHttps) {
            return new HttpUrl.Builder().scheme("https").host(this.host).port(this.port ?: 443).build().toString()
        }

        return new HttpUrl.Builder().scheme("http").host(this.host).port(this.port ?: 80)
    }

    String account() {
        return account
    }

    String password() {
        return password
    }

    String tenant() {
        return tenant
    }

    String host() {
        return host
    }

    int port() {
        return port
    }

    boolean enableHttps() {
        return enableHttps
    }

    static class Builder {

        String account
        String password
        String tenant
        String host
        int port
        boolean enableHttps

        Builder account(String account) {
            this.account = account
            return this
        }

        Builder password(String password) {
            this.password = password
            return this
        }

        Builder tenant(String tenant) {
            this.tenant = tenant
            return this
        }

        Builder host(String host) {
            this.host = host
            return this
        }

        Builder port(int port) {
            this.port = port
            return this
        }

        Builder enableHttps(boolean enableHttps) {
            this.enableHttps = enableHttps
            return this
        }

        ByzerClientConfig build() {
            return new ByzerClientConfig(account, password, tenant, host, port, enableHttps)
        }
    }

    static Builder builder() {
        return new Builder()
    }


}
