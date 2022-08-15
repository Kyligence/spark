def call() {
    timestamps {
        checkout([$class           : 'GitSCM', branches: [[name: '*/master']],
                  browser          : [$class: 'GogsGit', repoUrl: 'http://10.1.2.111:3003/git/kap_nodemodules.git'],
                  extensions       : [[$class: 'CloneOption', depth: 0, noTags: false, reference: '', shallow: true, timeout: 60],
                                      [$class: 'RelativeTargetDirectory', relativeTargetDir: 'kap_nodemodules'],
                                      [$class: 'CleanBeforeCheckout']],
                  userRemoteConfigs: [[credentialsId: 'gogs', url: 'http://10.1.2.111:3003/git/kap_nodemodules.git']]])
    }
}
