def call(String project, String branch = null, String schema = 'https', String remoteHost = 'github.com', refspec = '+refs/heads/*:refs/remotes/origin/*') {
    timestamps {
        checkout([$class           : 'GitSCM',
                  branches         : [[name: "${branch ?: params.branch ?: sha1}"]],
                  userRemoteConfigs: [[credentialsId: "${params.certificate ?: 'kyligencegit'}",
                                       name         : "origin",
                                       refspec      : "${refspec}",
                                       url          : "${schema}://${remoteHost}/${params.repo ?: 'Kyligence'}/${project}.git"]],
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
