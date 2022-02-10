def call(String project, String branch = null, String schema = 'git', String remoteHost = '10.1.2.192', refspec = '+refs/heads/*:refs/remotes/origin/*', timeout = 60) {
    timestamps {
        checkout([$class           : 'GitSCM',
                  branches         : [[name: "${branch ?: params.branch ?: sha1}"]],
                  userRemoteConfigs: [[credentialsId: "${params.certificate ?: 'kyligence-git'}",
                                       name         : "origin",
                                       refspec      : "${refspec}",
                                       url          : "${schema}://${remoteHost}/${params.repo ?: 'Kyligence'}/${project}.git"]],
                  browser          : [$class: 'GithubWeb', repoUrl: "https://github.com/Kyligence/${project}"],
                  extensions       : [
                          [$class: 'CleanBeforeCheckout'],
                          [$class: 'CloneOption', depth: 0, noTags: false, shallow: false, timeout: ${timeout ?: params.timeout ?: 60}}]
                  ]
        ])
    }
}
