def call(String project) {
    sshCmdWithCredentials(
        "sh /var/lib/git/pull-repo.sh ${project}",
        'private_git',
        ['name':'localhost', 'host':'10.1.2.192', 'allowAnyHosts': true]
    )
}