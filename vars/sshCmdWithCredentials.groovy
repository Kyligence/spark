def call(String cmd, String creId, remoteConf = [:]) {
    withCredentials([usernamePassword(credentialsId: "${creId}", passwordVariable: 'ssh_password', usernameVariable: 'ssh_username')]) {
        remoteConf.user = "${ssh_username}"
        remoteConf.password = "${ssh_password}"
        sshCommand remote: remoteConf, command: cmd
    }
}