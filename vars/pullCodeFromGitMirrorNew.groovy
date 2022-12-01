/**
 * Get code from Git Mirror
 * @param project the repo name
 * @param branch  the branch which will pull
 * @param fromRemote skip the mirror and pull the code from the remote
 */
def call(String project, String branch, Boolean fromRemote=false, String repo='Kyligence', String refspec = '+refs/heads/*:refs/remotes/origin/*', Boolean syncGitMirror=true, String certificate=null) {
    // println "pullCodeFromGitMirrorNew -> params: project: ${project}, branch/commit: ${branch}, fromRemote: ${fromRemote}, repo: ${repo}, refspec: ${refspec}, syncGitMirror: ${syncGitMirror}, certificate: ${certificate}"
    if (!branch) {
        error 'branch 参数不能为空'
    }

    if(!fromRemote) { // 从本地镜像获取
        if(syncGitMirror) {
            syncGitMirror(project)
        }

        println "checkout ${project} from git mirror at local, it'll be retry 3 times"
        retry(3) {
            // githubCheckoutNew project, branch, 'git', '10.1.2.192', null, certificate, repo
            githubCheckout project, branch, 'git', '10.1.2.192', refspec, certificate, repo
        }
    } else {
        println "checkout ${project} from github from remote, it'll be retry 3 times"
        retry(3) {
            // githubCheckoutNew project, branch, 'https', 'github.com', null, certificate, repo
            githubCheckout project, branch, 'https', 'github.com', refspec, certificate, repo
        }
    }
}