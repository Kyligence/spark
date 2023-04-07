package io.kyligence.devopslib.unit

import com.cloudbees.groovy.cps.NonCPS
import groovy.transform.builder.Builder
import io.kyligence.devopslib.Utils

class GithubCheckout extends Unit<GithubCheckoutParameter, Boolean> {

    @Builder
    static class GithubCheckoutParameter {

        String org

        String repo

        String cre

        String sha1

        String branch

    }

    GithubCheckout(String name) {
        super(name)
    }

    @Override
    void run(GithubCheckoutParameter p) {
        if (p.sha1) {
            Utils.log("checkout from pull request.")
            def pullNumber = matchPullNumber(p.sha1)

            Utils.ctx.checkout([$class: 'GitSCM', branches: [[name: p.sha1]], extensions: [[$class: 'CloneOption', depth: 1, honorRefspec: true, noTags: true, reference: '', shallow: true, timeout: 3]], userRemoteConfigs: [[credentialsId: "${p.cre}", name: 'origin', refspec: "+refs/pull/${pullNumber}/*:refs/remotes/origin/pr/${pullNumber}/*", url: "https://github.com/${p.org}/${p.repo}.git"]]])

        } else if (p.branch) {
            Utils.log("checkout from specified branch.")
            Utils.ctx.checkout([$class: 'GitSCM', branches: [[name: p.branch]], extensions: [[$class: 'CloneOption', depth: 1, honorRefspec: true, noTags: true, reference: '', shallow: true, timeout: 3]], userRemoteConfigs: [[credentialsId: "${p.cre}", name: 'origin', refspec: "+refs/heads/${p.branch}:refs/remotes/origin/${p.branch}", url: "https://github.com/${p.org}/${p.repo}.git"]]])

        } else {
            throw new RuntimeException("unknown branch or sha1")
        }

        this.setResult(true)
    }

    @NonCPS
    def matchPullNumber(String sha1) {
        def m = sha1 =~ /^origin\/pr\/(\d+)\/merge$/
        if (!m) {
            throw new RuntimeException("invalid sha1")
        }
        return m.group(1)
    }


    @Override
    boolean isSuccess() {
        return this.getResult() != null && this.getResult()
    }
}
