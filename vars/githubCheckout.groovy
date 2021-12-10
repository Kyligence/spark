def call(String project, String remoteHost = '10.1.2.192', refspec = '+refs/heads/*:refs/remotes/origin/*', String defaultBranch = 'master') {
    timestamps {
        checkout([$class           : 'GitSCM',
                  branches         : [[name: "${params.branch ?: sha1 ?: defaultBranch}"]],
                  userRemoteConfigs: [[credentialsId: "${params.certificate ?: 'kyligence-git'}",
                                       name         : "origin",
                                       refspec      : "${refspec}",
                                       url          : "git://${remoteHost}/${params.repo ?: 'Kyligence'}/${project}.git"]],
                  browser          : [$class: 'GithubWeb', repoUrl: "https://github.com/Kyligence/${project}"],
                  extensions       : [
                          [$class: 'CleanBeforeCheckout'],
                          [$class: 'CloneOption', depth: 0, noTags: false, shallow: false, timeout: 60]
                  ]
        ])
    }
}
