def call(String project, String branch = null, String schema = 'git', String remoteHost = '10.1.2.192', refspec = '+refs/heads/*:refs/remotes/origin/*', certificate=null, repo=null) {
    timestamps {
        if (sha1) {
            println "checkout from pull request."
            def pullNumber = matchPullNumber(sha1)
            checkout([$class: 'GitSCM', 
                      branches: [[name: sha1]], 
                      extensions: [[$class: 'CloneOption', depth: 1, honorRefspec: true, noTags: true, reference: '', shallow: true, timeout: 3]], 
                      userRemoteConfigs: [[credentialsId: "${certificate ?: 'kyligence-git'}",
                                          name: 'origin', 
                                          refspec: "+refs/pull/${pullNumber}/*:refs/remotes/origin/pr/${pullNumber}/*", 
                                          url: "${schema ?: 'git'}://${remoteHost ?: '10.1.2.192'}/${repo ?: 'Kyligence'}/${project}.git"]]])
        } else if (params.branch) {
            println "checkout from specified branch."
            checkout([$class: 'GitSCM', 
                      branches: [[name: params.branch]], 
                      extensions: [[$class: 'CloneOption', depth: 1, honorRefspec: true, noTags: true, reference: '', shallow: true, timeout: 3]], 
                      userRemoteConfigs: [[credentialsId: "${certificate ?: 'kyligence-git'}", 
                                          name: 'origin', 
                                          refspec: "+refs/heads/${params.branch}:refs/remotes/origin/${params.branch}", 
                                          url: "${schema ?: 'git'}://${remoteHost ?: '10.1.2.192'}/${repo ?: 'Kyligence'}/${project}.git"]]])
        } else {
            throw new RuntimeException("unknown branch")
        }
        checkLicensePatch()
    }
}

def checkLicensePatch() {
    sh script: '''
    if [ -f license.patch ]; then
        git apply license.patch
        git checkout -- .
    fi
    '''
}

def matchPullNumber(String sha1) {
    def m = sha1 =~ /^origin\/pr\/(\d+)\/merge$/
    if (!m) {
        throw new RuntimeException("invalid sha1")
    }
    return m.group(1)
}