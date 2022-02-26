def call(String sonarArgLine) {
    container("sonar-cloud") {
        dir("sourcecode") {
            withSonarQubeEnv(credentialsId: 'sonar_credential', installationName: 'sonar cloud') {
                // You can override the credential to be used
                sh script: """
                    mkdir -p coverage/exec
                    mkdir -p coverage/jars
                    i=1
                    outputs=\$(find "${WORKSPACE}" -path '*Test-Stage-*jacoco.exec' | sed 's/.*/&/')
                    for out in \$outputs; do
                        target_out=\$(echo \$out | rev |cut -d/ -f 3 | rev)
                        cp -r \$out coverage/exec/"\$target_out-\$i-jacoco.exec"
                        i=\$((i+1))
                    done
                """

                dir("coverage") {
                    script {
                        try { sh script: "cp ../src/server/target/jars/kap-* jars/" } catch (Exception err) { }
                        try { sh script: "cp ../src/server/target/jars/yinglong-* jars/" } catch (Exception err) { }
                        try { sh script: "cp ../src/server/target/jars/ke-* jars/" } catch (Exception err) { }
                        try { sh script: "rm -f jars/kap-external-curator-* jars/kap-external-guava20-* jars/kap-external-swagger-* jars/kap-external-influxdb-*" } catch (Exception err) { }
                        try { sh script: "rm -f jars/*-tests.jar" } catch (Exception err) { }
                        sh script: "java -jar jacococli.jar report ./exec/* --html output --classfiles jars/ --xml jacoco.xml"
                    }
                }
                sh script: """
                    mvn sonar:sonar -T 4C \
                        -Dsonar.host.url=https://sonarcloud.io \
                        -Dsonar.organization=kyligence \
                        -Dsonar.projectKey=Kyligence_KAP \
                        ${sonarArgLine} \
                        -Dsonar.coverage.jacoco.xmlReportPaths=${WORKSPACE}/sourcecode/coverage/jacoco.xml
                """
            }
        }
    }
}