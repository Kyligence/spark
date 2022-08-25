def copy_kybot(hadoop_release) {
    switch (hadoop_release) {
        case 'hdp-fi':
        case 'mrs':
            sh script: """
                cp -rn /mnt/jenkins/kybot/kybot-ke-master/kybot-client-*-hbase1.x-bin.tar.gz build/
            """
            break
        case 'cdh':
        case 'cdh6':
        case 'cdh7':
        case 'hdp3':
        case 'mapr':
            sh script: """
                cp -rn /mnt/jenkins/kybot/kybot-ke-master/kybot-client-*-cdh5.7-bin.tar.gz build/
            """
            break
    }
}

def build(hadoop_release, ops) {
    println "hadoop_release: ${hadoop_release}"
    println "ops: ${ops}"
    def internal_build = { it ->
        sh script: "build/script/release.sh ${ops.skipObf ? '-skipObf' : ''} ${ops.noPlus ? '-noPlus' : ''} ${ops.skipFront ? '-skipFront' : ''} -P ${it} || exit 1"
    }
    switch (hadoop_release) {
        case 'hdp-fi':
            internal_build 'mrs'
            break
        case 'cdh':
        case 'cdh6':
            internal_build 'cdh5.7'
            break
        case 'cdh7':
            internal_build 'cdh7'
        case 'hdp3':
            internal_build 'hdp3'
        case 'mapr':
            internal_build 'mapr'
        case 'mrs':
            internal_build 'mrs'
    }
}

def git_archive(hadoop_release, release_series) {
    switch (hadoop_release) {
        case 'hdp-fi':
        case 'mrs':
            sh script: """
                git archive HEAD --prefix="Kyligence-Enterprise-src/" --format=zip > dist/Kyligence-Enterprise-src-${release_series}-hbase1.x.zip
            """
            break
        case 'cdh':
            sh script: """
                git archive HEAD --prefix="Kyligence-Enterprise-src/" --format=zip > dist/Kyligence-Enterprise-src-${release_series}-cdh5.7.zip
            """
            break
        case 'cdh6':
            sh script: """
                git archive HEAD --prefix="Kyligence-Enterprise-src/" --format=zip > dist/Kyligence-Enterprise-src-${release_series}-cdh6.zip
            """
            break
        case 'cdh7':
            sh script: """
                git archive HEAD --prefix="Kyligence-Enterprise-src/" --format=zip > dist/Kyligence-Enterprise-src-${release_series}-cdh7.zip
            """
            break
        case 'hdp3':
            sh script: """
                git archive HEAD --prefix="Kyligence-Enterprise-src/" --format=zip > dist/Kyligence-Enterprise-src-${release_series}-hdp3.zip
            """
            break
        case 'mapr':
            sh script: """
                git archive HEAD --prefix="Kyligence-Enterprise-src/" --format=zip > dist/Kyligence-Enterprise-src-${release_series}-mapr.zip
            """
            break
    }
}

def smoke_test(hadoop_release, skip_engine, boolean plus) {
    switch (hadoop_release) {
        case 'hdp-fi':
        case 'mrs':
            sh script: """
                build/script/${plus ? 'jenkins-smoke-test-release-plus.sh' : 'jenkins-smoke-test-release.sh'} HDP ${skip_engine} || exit 1
            """
            break
        case 'cdh':
        case 'cdh6':
        case 'cdh7':
        case 'hdp3':
        case 'mapr':
            sh script: """
                build/script/${plus ? 'jenkins-smoke-test-release-plus.sh' : 'jenkins-smoke-test-release.sh'} || exit 1
            """
            break
    }
}

def release_noplus(hadoop_release, release_series) {
    switch (hadoop_release) {
        case 'hdp-fi':
            sh script: """
                cp -f dist/Kyligence-Enterprise-*-hdp-orig.tar.gz /mnt/jenkins/latest-packages/Kyligence-Enterprise-hdp-orig.tar.gz || exit 1
                cp -f dist/Kyligence-Enterprise-*-hdp.tar.gz /mnt/jenkins/latest-packages/Kyligence-Enterprise-hdp.tar.gz || exit 1
                rm dist/Kyligence-Enterprise-*-orig.tar.gz || exit 1
                scp -r dist 10.1.1.28:/root/kap/release/${release_series}-hbase1.x/kap-${release_series}-hbase1.x-$BUILD_NUMBER/ || exit 1
                ssh 10.1.1.28 "ln -f /root/kap/release/${release_series}-hbase1.x/kap-${release_series}-hbase1.x-$BUILD_NUMBER/Kyligence-Enterprise-*-hdp.tar.gz /root/kap/release/${release_series}-hbase1.x/Kyligence-Enterprise-${release_series}-hdp-latest.tar.gz"
    
                rm -rf dist || exit 1
                rm -f LICENSE || exit 1
            """
            break
        case 'cdh':
            sh script: """
                cp -f dist/Kyligence-Enterprise-*-cdh5.7-orig.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                cp -f dist/Kyligence-Enterprise-*-cdh5.7.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                rm dist/Kyligence-Enterprise-*-cdh5.7-orig.tar.gz || exit 1
                scp -r dist root@10.1.1.28:/root/kap/release/${release_series}-cdh5.7/kap-${release_series}-cdh5.7-$BUILD_NUMBER/ || exit 1
            """
            break
        case 'cdh6':
            sh script: """
                cp -f dist/Kyligence-Enterprise-*-cdh6-orig.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                cp -f dist/Kyligence-Enterprise-*-cdh6.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                rm dist/Kyligence-Enterprise-*-cdh6-orig.tar.gz || exit 1
                scp -r dist root@10.1.1.28:/root/kap/release/${release_series}-cdh6.0/kap-${release_series}-cdh6.0-$BUILD_NUMBER/ || exit 1
            """
            break
        case 'cdh7':
            sh script: """
                cp -f dist/Kyligence-Enterprise-*-cdh7-orig.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                cp -f dist/Kyligence-Enterprise-*-cdh7.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                rm dist/Kyligence-Enterprise-*-cd7-orig.tar.gz || exit 1
                scp -r dist root@10.1.1.28:/root/kap/release/${release_series}-cdh7/kap-${release_series}-cdh7-$BUILD_NUMBER/ || exit 1
            """
            break
        case 'hdp3':
            sh script: """
                cp -f dist/Kyligence-Enterprise-*-hdp3-orig.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                cp -f dist/Kyligence-Enterprise-*-hdp3.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                rm dist/Kyligence-Enterprise-*-hdp3-orig.tar.gz || exit 1
                scp -r dist root@10.1.1.28:/root/kap/release/${release_series}-hdp3/kap-${release_series}-hdp3-$BUILD_NUMBER/ || exit 1
            """
            break
        case 'mapr':
            sh script: """
                rm dist/Kyligence-Enterprise-*-mapr-orig.tar.gz || exit 1
                scp -r dist root@10.1.1.28:/root/kap/release/${release_series}-mapr/kap-${release_series}-mapr-$BUILD_NUMBER/ || exit 1
            """
            break
        case 'mrs':
            sh script: """
                cp -f dist/Kyligence-Enterprise-*-mrs-orig.tar.gz /mnt/jenkins/latest-packages/Kyligence-Enterprise-mrs-orig.tar.gz || exit 1
                cp -f dist/Kyligence-Enterprise-*-mrs.tar.gz /mnt/jenkins/latest-packages/Kyligence-Enterprise-mrs.tar.gz || exit 1
                rm dist/Kyligence-Enterprise-*-orig.tar.gz || exit 1
                scp -r dist 10.1.1.28:/root/kap/release/${release_series}-hbase1.x/kap-${release_series}-hbase1.x-mrs-$BUILD_NUMBER/ || exit 1
                ssh 10.1.1.28 "ln -f /root/kap/release/${release_series}-hbase1.x/kap-${release_series}-hbase1.x-mrs-$BUILD_NUMBER/Kyligence-Enterprise-*-mrs.tar.gz /root/kap/release/${release_series}-hbase1.x/Kyligence-Enterprise-${release_series}-mrs-latest.tar.gz"
                  
                rm -rf dist || exit 1
                rm -f LICENSE || exit 1
            """
            break
    }
}

def release_plus(hadoop_release, release_version, release_series) {
    switch (hadoop_release) {
        case 'hdp-fi':
            sh script: """
                cp -f dist/Kyligence-Enterprise-*-hdp-orig.tar.gz /mnt/jenkins/latest-packages/Kyligence-Enterprise-hdp-orig.tar.gz || exit 1
                cp -f dist/Kyligence-Enterprise-*-hdp.tar.gz /mnt/jenkins/latest-packages/Kyligence-Enterprise-hdp.tar.gz || exit 1
                rm dist/Kyligence-Enterprise-*-orig.tar.gz || exit 1
                scp -r dist 10.1.1.28:/root/kap/release/\${release_series}-hbase1.x/kap-plus-\${release_series}-hbase1.x-\$BUILD_NUMBER/ || exit 1
            """

            if (release_version ==~ /.*GA.*/) {
                sh script: """
                    scp -r root@10.1.1.28:/root/kap/release/\${release_series}-hbase1.x/kap-plus-\${release_series}-hbase1.x-\$BUILD_NUMBER/Kyligence-Enterprise-*-hdp.tar.gz   root@10.1.2.171:/root/QA/GA_packages/3x/hdp/  || exit 1
                    scp -r root@10.1.1.28:/root/kap/release/\${release_series}-hbase1.x/kap-plus-\${release_series}-hbase1.x-\$BUILD_NUMBER/Kyligence-Enterprise-*-fi.tar.gz   root@10.1.2.171:/root/QA/GA_packages/3x/fi/  || exit 1
                """
            } else if (release_version ==~ /.*RC.*/) {
                sh script: """
                    scp -r root@10.1.1.28:/root/kap/release/${release_series}-hbase1.x/kap-plus-${release_series}-hbase1.x-$BUILD_NUMBER/Kyligence-Enterprise-*-hdp.tar.gz   root@10.1.2.171:/root/QA/RC_packages/3x/hdp/  || exit 1
                    scp -r root@10.1.1.28:/root/kap/release/${release_series}-hbase1.x/kap-plus-${release_series}-hbase1.x-$BUILD_NUMBER/Kyligence-Enterprise-*-fi.tar.gz   root@10.1.2.171:/root/QA/RC_packages/3x/fi/  || exit 1
                """
            }

            sh script: """
                ssh 10.1.1.28 "ln -f /root/kap/release/\${release_series}-hbase1.x/kap-plus-\${release_series}-hbase1.x-\$BUILD_NUMBER/Kyligence-Enterprise-*-hdp.tar.gz /root/kap/release/\${release_series}-hbase1.x/Kyligence-Enterprise-\${release_series}-hdp-latest.tar.gz"
                rm -f LICENSE || exit 1
            """
            break
        case 'cdh':
            sh script: """
                cp -f dist/Kyligence-Enterprise-*-cdh-orig.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                cp -f dist/Kyligence-Enterprise-*-cdh.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                rm dist/Kyligence-Enterprise-*-orig.tar.gz || exit 1
                scp -r dist root@10.1.1.28:/root/kap/release/${release_series}-cdh5.7/kap-plus-${release_series}-cdh5.7-$BUILD_NUMBER/ || exit 1
            """

            if (release_version ==~ /.*GA.*/) {
                sh script: """
                    scp -r root@10.1.1.28:/root/kap/release/${release_series}-cdh5.7/kap-plus-${release_series}-cdh5.7-$BUILD_NUMBER/Kyligence-Enterprise-*-cdh.tar.gz  root@10.1.2.171:/root/QA/GA_packages/3x/cdh/  || exit 1
                """
            } else if (release_version ==~ /.*RC.*/) {
                sh script: """
                    scp -r root@10.1.1.28:/root/kap/release/${release_series}-cdh5.7/kap-plus-${release_series}-cdh5.7-$BUILD_NUMBER/Kyligence-Enterprise-*-cdh.tar.gz  root@10.1.2.171:/root/QA/RC_packages/3x/cdh/  || exit 1
                """
            }

            sh script: """
                ssh 10.1.1.28 "ln -f /root/kap/release/${release_series}-cdh5.7/kap-plus-${release_series}-cdh5.7-$BUILD_NUMBER/Kyligence-Enterprise-*-cdh.tar.gz /root/kap/release/${release_series}-cdh5.7/Kyligence-Enterprise-${release_series}-cdh-latest.tar.gz"
                rm -f LICENSE || exit 1
            """
            break
        case 'cdh6':
            sh script: """
                cp -f dist/Kyligence-Enterprise-*-cdh6-orig.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                cp -f dist/Kyligence-Enterprise-*-cdh6.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                rm dist/Kyligence-Enterprise-*-orig.tar.gz || exit 1
                scp -r dist root@10.1.1.28:/root/kap/release/${release_series}-cdh6.0/kap-plus-${release_series}-cdh6.0-$BUILD_NUMBER/ || exit 1
            """

            if (release_version ==~ /.*GA.*/) {
                sh script: """
                    scp -r root@10.1.1.28:/root/kap/release/${release_series}-cdh6.0/kap-plus-${release_series}-cdh6.0-$BUILD_NUMBER/Kyligence-Enterprise-*.tar.gz   root@10.1.2.171:/root/QA/GA_packages/3x/cdh6/  || exit 1
                """
            } else if (release_version ==~ /.*RC.*/) {
                sh script: """
                    scp -r root@10.1.1.28:/root/kap/release/${release_series}-cdh6.0/kap-plus-${release_series}-cdh6.0-$BUILD_NUMBER/Kyligence-Enterprise-*.tar.gz   root@10.1.2.171:/root/QA/RC_packages/3x/cdh6/  || exit 1
                """
            }

            sh script: """
                ssh 10.1.1.28 "ln -f /root/kap/release/${release_series}-cdh6.0/kap-plus-${release_series}-cdh6.0-$BUILD_NUMBER/Kyligence-Enterprise-*-cdh6.tar.gz /root/kap/release/${release_series}-cdh6.0/Kyligence-Enterprise-${release_series}-cdh6-latest.tar.gz"
                rm -rf dist || exit 1
                rm -f LICENSE || exit 1
            """
            break
        case 'cdh7':
            sh script: """
                cp -f dist/Kyligence-Enterprise-*-cdh7-orig.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                cp -f dist/Kyligence-Enterprise-*-cdh7.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                rm dist/Kyligence-Enterprise-*-orig.tar.gz || exit 1
                scp -r dist root@10.1.1.28:/root/kap/release/${release_series}-cdh7/kap-plus-${release_series}-cdh7-$BUILD_NUMBER/ || exit 1
            """

            if (release_version ==~ /.*GA.*/) {
                sh script: """
                    scp -r root@10.1.1.28:/root/kap/release/${release_series}-cdh7/kap-plus-${release_series}-cdh7-$BUILD_NUMBER/Kyligence-Enterprise-*.tar.gz   root@10.1.2.171:/root/QA/GA_packages/3x/cdh7/  || exit 1
                """
            } else if (release_version ==~ /.*RC.*/) {
                sh script: """
                    scp -r root@10.1.1.28:/root/kap/release/${release_series}-cdh7/kap-plus-${release_series}-cdh7-$BUILD_NUMBER/Kyligence-Enterprise-*.tar.gz   root@10.1.2.171:/root/QA/RC_packages/3x/cdh7/  || exit 1
                """
            }

            sh script: """
                ssh 10.1.1.28 "ln -f /root/kap/release/${release_series}-cdh7/kap-plus-${release_series}-cdh7-$BUILD_NUMBER/Kyligence-Enterprise-*-cdh7.tar.gz /root/kap/release/${release_series}-cdh7/Kyligence-Enterprise-${release_series}-cdh7-latest.tar.gz"
                rm -rf dist || exit 1
                rm -f LICENSE || exit 1
            """
            break
        case 'hdp3':
            sh script: """
                cp -f dist/Kyligence-Enterprise-*-hdp3-orig.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                cp -f dist/Kyligence-Enterprise-*-hdp3.tar.gz /mnt/jenkins/latest-packages/ || exit 1
                rm dist/Kyligence-Enterprise-*-orig.tar.gz || exit 1
                scp -r dist root@10.1.1.28:/root/kap/release/${release_series}-hdp3/kap-plus-${release_series}-hdp3-$BUILD_NUMBER/ || exit 1
            """

            if (release_version ==~ /.*GA.*/) {
                sh script: """
                    scp -r root@10.1.1.28:/root/kap/release/${release_series}-hdp3/kap-plus-${release_series}-hdp3-$BUILD_NUMBER/Kyligence-Enterprise-*.tar.gz   root@10.1.2.171:/root/QA/GA_packages/3x/hdp3/  || exit 1
                """
            } else if (release_version ==~ /.*RC.*/) {
                sh script: """
                    scp -r root@10.1.1.28:/root/kap/release/${release_series}-hdp3/kap-plus-${release_series}-hdp3-$BUILD_NUMBER/Kyligence-Enterprise-*.tar.gz   root@10.1.2.171:/root/QA/RC_packages/3x/hdp3/  || exit 1
                """
            }

            sh script: """
                ssh 10.1.1.28 "ln -f /root/kap/release/${release_series}-hdp3/kap-plus-${release_series}-hdp3-$BUILD_NUMBER/Kyligence-Enterprise-*-hdp3.tar.gz /root/kap/release/${release_series}-hdp3/Kyligence-Enterprise-${release_series}-hdp3-latest.tar.gz"
                rm -rf dist || exit 1
                rm -f LICENSE || exit 1
            """
            break
        case 'mapr':
            sh script: """
                rm dist/Kyligence-Enterprise-*-orig.tar.gz || exit 1
                scp -r dist root@10.1.1.28:/root/kap/release/${release_series}-mapr/kap-plus-${release_series}-mapr-$BUILD_NUMBER/ || exit 1
            """

            if (release_version ==~ /.*GA.*/) {
                sh script: """
                    scp -r root@10.1.1.28:/root/kap/release/${release_series}-mapr/kap-plus-${release_series}-mapr-$BUILD_NUMBER/Kyligence-Enterprise-*.tar.gz   root@10.1.2.171:/root/QA/GA_packages/3x/mapr/  || exit 1
                """
            } else if (release_version ==~ /.*RC.*/) {
                sh script: """
                    scp -r root@10.1.1.28:/root/kap/release/${release_series}-mapr/kap-plus-${release_series}-mapr-$BUILD_NUMBER/Kyligence-Enterprise-*.tar.gz   root@10.1.2.171:/root/QA/RC_packages/3x/mapr/  || exit 1
                """
            }

            sh script: """
                ssh 10.1.1.28 "ln -f /root/kap/release/${release_series}-mapr/kap-plus-${release_series}-mapr-$BUILD_NUMBER/Kyligence-Enterprise-*-mapr.tar.gz /root/kap/release/${release_series}-mapr/Kyligence-Enterprise-${release_series}-mapr-latest.tar.gz"
                rm -rf dist || exit 1
                rm -f LICENSE || exit 1
            """
            break
        case 'mrs':
            sh script: """
                cp -f dist/Kyligence-Enterprise-*-mrs-orig.tar.gz /mnt/jenkins/latest-packages/Kyligence-Enterprise-mrs-orig.tar.gz || exit 1
                cp -f dist/Kyligence-Enterprise-*-mrs.tar.gz /mnt/jenkins/latest-packages/Kyligence-Enterprise-mrs.tar.gz || exit 1
                rm dist/Kyligence-Enterprise-*-orig.tar.gz || exit 1
                scp -r dist 10.1.1.28:/root/kap/release/${release_series}-hbase1.x/kap-plus-${release_series}-hbase1.x-mrs-$BUILD_NUMBER/ || exit 1
            """

            if (release_version ==~ /.*GA.*/) {
                sh script: """
                    scp -r root@10.1.1.28:/root/kap/release/${release_series}-hbase1.x/kap-plus-${release_series}-hbase1.x-mrs-$BUILD_NUMBER/Kyligence-Enterprise-*-mrs.tar.gz   root@10.1.2.171:/root/QA/GA_packages/3x/mrs/  || exit 1
                """
            } else if (release_version ==~ /.*RC.*/) {
                sh script: """
                    scp -r root@10.1.1.28:/root/kap/release/${release_series}-hbase1.x/kap-plus-${release_series}-hbase1.x-mrs-$BUILD_NUMBER/Kyligence-Enterprise-*-mrs.tar.gz   root@10.1.2.171:/root/QA/RC_packages/3x/mrs/  || exit 1
                """
            }

            sh script: """
                ssh 10.1.1.28 "ln -f /root/kap/release/${release_series}-hbase1.x/kap-plus-${release_series}-hbase1.x-mrs-$BUILD_NUMBER/Kyligence-Enterprise-*-mrs.tar.gz /root/kap/release/${release_series}-hbase1.x/Kyligence-Enterprise-${release_series}-mrs-latest.tar.gz"
                rm -f LICENSE || exit 1
            """
            break
    }
}

def gen_license() {
    sh script: """

        cat << EOF > trial-license.cfg
License for Kyligence Enterprise Evaluation.
Category:3.x
Volume:10
Service Nodes:5
====
version=Kyligence Enterprise \${release_version/-/ }
EOF

        kap_commit_sha1=`git rev-parse HEAD`
        kylin_commit_sha1=`git submodule status kylin`
        kylin_commit_sha1=\${kylin_commit_sha1:1:40}
        exp_date=`date -d "+1 month" +%Y-%m-%d`

        echo "kap-commit=\${kap_commit_sha1}" >> trial-license.cfg
        echo "kylin-commit=\${kylin_commit_sha1}" >> trial-license.cfg
        echo "exp-date=\${exp_date}" >> trial-license.cfg

        curl -H "Authorization: Basic amVua2luczpLeUJvdElzQmVzdDIwMTY=" -F cfg=@trial-license.cfg http://priv-lic5042.chinacloudapp.cn:18086/api/license/kap -o LICENSE

        if [ -z `grep -l "====" LICENSE` ]; then
            echo "license not created."
            exit 1;
        fi
    """
}
