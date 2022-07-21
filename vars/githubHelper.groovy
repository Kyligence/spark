def call(String ctl, String base, String project, String tag=null,
        String repo='Kyligence', Boolean isCloned=true, String credentialsId='kyligence-git') {
    switch (ctl) {
        case 'INIT':
            gitInit(base, project, repo)
            break
        case 'TAG':
            gitTag(base, tag, project, repo, isCloned)
            break
        case 'PR':
            createPR(base, project, repo, tag, isCloned)
            break
        case 'BRANCH':
            createBranch(base, project, repo, tag, isCloned)
            break
        case 'COMMITID':
            gitCommitId(isShort)
            break
    }
}

def gitCommitId(isShort=true) {
    println "gitCommitId -> isShort[${isShort}]"
    def shortStr = isShort ? '--short' : ''
    return sh(returnStdout:true, script: "git rev-parse ${shortStr} HEAD | sed -e 's/\r//g'").trim()
}

// git init
def gitInit(base, project='KAP', repo='Kyligence') {
    println "gitInit -> base[${base}], project[${project}], repo[${repo}]"
    withCredentials([string(credentialsId: 'jenkins-token', variable: 'TOKEN')]) {
        println "https://${TOKEN}@github.com/${repo}/${project}.git"
        sh "echo '#!/bin/bash' >> credential-helper.sh"
        sh "echo 'echo username=\$GIT_USERNAME' >> credential-helper.sh"
        sh "echo 'echo password=\$GIT_PASSWORD' >> credential-helper.sh"
        sh 'cat credential-helper.sh'
        withCredentials([usernamePassword(credentialsId: credentialsId,
                        usernameVariable: 'GIT_USERNAME',
                        passwordVariable: 'GIT_PASSWORD')]) {
            sh script:"""git init .
                git config credential.helper "/bin/bash credential-helper.sh"
                git config --global init.defaultBranch ${base}
                git remote add origin https://${TOKEN}@github.com/${repo}/${project}.git
                git branch -m ${base}
                git fetch --all
                git branch ${base} origin/${base}
            """
                        }
    }
}

// git tag
def gitTag(base, tag, project='KAP', repo='Kyligence', isCloned=true) {
    println "gitTag -> base[${base}], tag[${tag}], project[${project}], repo[${repo}]"
    if (!tag) {
        error('The tag cannot null')
    }
    if (!isCloned) {
        gitInit(base, project, repo)
    }

    withCredentials([string(credentialsId: 'jenkins-token', variable: 'TOKEN')]) {
        // println "https://${TOKEN}@github.com/${repo}/${project}.git"
        sh script: """
            git remote remove origin
            git remote add origin https://${TOKEN}@github.com/${repo}/${project}.git
        """

        try{
            println "尝试删除旧tag:【${tag}】"
            sh script: """
                git show ${tag}
                git tag -d ${tag}
                git push origin :refs/tags/${tag}
            """
            println "成功删除旧tag:【${tag}】"
        } catch(ex) {
            echo """Exception: \n ${ex.toString()}\n \
                    ooops - caught: ${ex.class}\n \
                    ooops - with msg: ${ex.message}\n \
                    ooops - backtrace: ${ex.stackTrace}"""
        }

        sh script: """
            git tag ${tag}
            git push origin ${tag}
            git show ${tag}
        """
    }
}

// not ready
def createPR(base, head, project='KAP', repo='Kyligence') {
    def repoURL = "https://github.com/${repo}/${projetc}"
    println "createPR -> base[${base}], head[${head}], project[${project}], repo[${repo}], repoURL[${repoURL}]"
    withCredentials([usernamePassword(credentialsId: 'kyligence-git', usernameVariable: 'GITHUB_APP', passwordVariable: 'GITHUB_ACCESS_TOKEN')]) {
        def prScript= """curl -H "Content-Type: application/json" \
        -H "Accept: application/vnd.github.v3+json" \
        -H "authorization: Bearer ${GITHUB_ACCESS_TOKEN}" \
        -X POST \
        ${repoURL}/pulls \
        -d '{ \
            "title": "from ${head} to ${base}", \
            "head": "${head}", \
            "base": "${base}" \
        }'"""
        print prScript
        def result = sh(script: prScript, returnStdout: true)
        def jsonObj = readJSON text: result
        if (jsonObj['html_url'] != null) {
            return jsonObj['html_url']
        } else {
            return it
        }
    }
}

// not ready
def createBranch(base, branch, project='KAP', repo='Kyligence', isCloned=true) {
    println "createBranch -> base[${base}], branch[${branch}], project[${project}], repo[${repo}]"
    if (!isCloned) {
        gitInit(base, project, repo)
    }
    sh script:"""
        git checkout -b ${branch} origin/${base}
        git push --set-upstream origin ${branch}
    """
}
