def call(String project, String branch = null, String schema = 'git', String remoteHost = '10.1.2.192', refspec = '+refs/heads/*:refs/remotes/origin/*', certificate=null, repo=null) {
    def _refspec = ''
    timestamps {
        checkout([
            $class: 'GitSCM', 
            branches: [[name: "${branch ?: sha1}"]], 
            extensions: [[$class: 'CloneOption', depth: 1, honorRefspec: true, noTags: true, reference: '', shallow: true, timeout: 3]], 
            userRemoteConfigs: [[credentialsId: "${certificate ?: 'kyligence-git'}", name: 'origin', 
                                refspec: "${branch ? '+refs/heads/'+branch+':refs/remotes/origin/'+branch : '+refs/pull/'+matchPullNumber(sha1)+'/*:refs/remotes/origin/pr/'+matchPullNumber(sha1)+'/*'}", 
                                url: "${schema ?: 'git'}://${remoteHost ?: '10.1.2.192'}/${repo ?: 'Kyligence'}/${project}.git"]]])
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