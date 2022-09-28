def call(String sonarArgLine) {
    container('sonar-cloud') {
        dir('sourcecode') {
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

                def copyJars(path) {
                    try { 
                        sh script: """
                        cp ${path} jars/
                        ls -l ${path} | grep "^-" | wc -l
                        """
                    } catch (Exception err) { }
                }

                dir('coverage') {
                    script {
                        sh script: 'pwd'
                        copyJars('../src/server/target/jars/kap-*')
                        copyJars('../src/server/target/jars/yinglong-*')
                        copyJars('../src/server/target/jars/kylin-*')
                        copyJars('../src/server/target/jars/ke-*')

                        //  --- start ---  only yinglong-open-core. if error, ignore
                        if(fileExists("../kylin")) {
                            // try { sh script: 'cp ../kylin/src/server/target/jars/kap-* jars/' } catch (Exception err) { }
                            // try { sh script: "cp ../kylin/src/server/target/jars/kylin-* jars/" } catch (Exception err) { }
                            // try { sh script: "cp ../kylin/src/second-storage/clickhouse-it/target/kylin-* jars/" } catch (Exception err) { }
                            // try { sh script: "cp ../kylin/src/spark-project/spark-it/target/spark-it-* jars/" } catch (Exception err) { }
                            copyJars('../kylin/src/*/target/k*')
                            copyJars('../kylin/src/*/*/target/k*')
                        }

                        if(fileExists("../kyligence")) {
                            copyJars('../kyligence/src/*/target/k*')
                            copyJars('../kyligence/src/*/*/target/k*')
                            // try { sh script: "cp ../kyligence/src/spark-project/*/target/spark-it-* jars/" } catch (Exception err) { }
                        }
                        // --- end ---

                        try { sh script: 'rm -f jars/kap-external-curator-* jars/kap-external-guava20-* jars/kap-external-swagger-* jars/kap-external-influxdb-*' } catch (Exception err) { }
                        try { sh script: 'rm -f jars/*-tests.jar' } catch (Exception err) { }
                        try { sh script: 'rm -f jars/*-assembly-*.jar' } catch (Exception err) { }
                        retry(3){
                            sh script: '''
                                java -jar jacococli.jar report ./exec/* --html output --classfiles jars/ --xml jacoco.xml
                                ls -l jars/*.jar | grep "^-" | wc -l
                            '''
                        }
                    }
                }
                retry(3) {
                    sh script: """
                        mvn sonar:sonar -T 4C \
                            -Dsonar.host.url=https://sonarcloud.io \
                            -Dsonar.organization=kyligence \
                            ${sonarArgLine} \
                            -Dsonar.coverage.jacoco.xmlReportPaths=${WORKSPACE}/sourcecode/coverage/jacoco.xml
                    """
                }
            }
        }
    }
}
