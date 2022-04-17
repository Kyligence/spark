def call(String project, String branch, Boolean fromRemote=false) {
    println "pullCodeFromGitMirror -> params: project: ${project}, branch/commit: ${branch}, fromRemote: ${fromRemote}"
    if (!branch) {
        error 'branch 参数不能为空'
    }

    if(!fromRemote) { // 从本地镜像获取
        println 'Sync the git mirror'
        sshCmdWithCredentials(
            'bash -c /var/lib/git/pull.sh',
            'private_git',
            ['name':'localhost', 'host':'10.1.2.192', 'allowAnyHosts': true]
        )

        println "checkout ${project} from git mirror at local, it'll be retry 3 times"
        retry(3) {
            githubCheckout project, branch
        }
    } else {
        println "checkout ${project} from github from remote, it'll be retry 3 times"
        retry(3) {
            githubCheckout project, branch, 'https', 'github.com'
        }
    }
}