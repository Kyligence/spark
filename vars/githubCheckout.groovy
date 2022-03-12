def call(String project, String branch = null, String schema = 'git', String remoteHost = '10.1.2.192', refspec = '+refs/heads/*:refs/remotes/origin/*', certificate=null, repo=null) {
    timestamps {
        checkout([$class           : 'GitSCM',
                  branches         : [[name: "${branch ?: sha1}"]],
                  userRemoteConfigs: [[credentialsId: "${certificate ?: 'kyligence-git'}",
                                       name         : "origin",
                                       refspec      : "${refspec}",
                                       url          : "${schema ?: 'git'}://${remoteHost ?: '10.1.2.192'}/${repo ?: 'Kyligence'}/${project}.git"]],
                  browser          : [$class: 'GithubWeb', repoUrl: "https://github.com/Kyligence/${project}"],
                  extensions       : [
                          [$class: 'CleanBeforeCheckout'],
                          [$class: 'CloneOption', depth: 0, noTags: false, shallow: false, timeout: 60]
                  ]
        ])
        sh script: '''
        if [ -f license.patch ]; then
            git apply license.patch
            git checkout -- .
        fi
        '''
    }
}
