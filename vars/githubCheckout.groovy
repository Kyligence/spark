def call(String project, String remoteHost = '10.1.2.192') {
    timestamps {
        checkout([$class           : 'GitSCM',
                  branches         : [[name: "${params.branch ?: sha1}"]],
                  userRemoteConfigs: [[credentialsId: "${params.certificate ?: 'kyligence-git'}",
                                       url          : "git://${remoteHost}/${params.repo ?: 'Kyligence'}/${project}.git"]],
                  browser          : [$class: 'GithubWeb', repoUrl: "https://github.com/Kyligence/${project}"],
                  extensions       : [
                          [$class: 'CleanBeforeCheckout'],
                          [$class: 'CloneOption', depth: 0, noTags: false, shallow: false, timeout: 60]
                  ]
        ])
    }
}
