def call(String version, boolean skipPatch = true, boolean skipLicense = true) {
    if (!skipPatch) {
        sh 'git apply /var/cicd/patchs/newten.patch'
    }

    if (!skipLicense) {
        sh script
        """
            cat << EOF > trial-license.cfg
License for Kyligence Enterprise Evaluation.
Category:4.x
Volume:10
Service Nodes:5
====
version=Kyligence Enterprise ${version}
EOF
            kap_commit_sha1=`git rev-parse HEAD`
            exp_date=`date -d "+1 month" +%Y-%m-%d`
            echo "kap-commit=\${kap_commit_sha1}" >> trial-license.cfg
            echo "exp-date=\${exp_date}" >> trial-license.cfg
            curl -H "Authorization: Basic amVua2luczpLeUJvdElzQmVzdDIwMTY=" -F cfg=@trial-license.cfg http://priv-lic5042.chinacloudapp.cn:18086/api/license/kap -o LICENSE
            if [ -z `grep -l "====" LICENSE` ]; then
                echo "license not created."
                exit 1
            fi
        """
    }

    sh "export release_version=${version} && sh build/script_newten/release.sh -noTimestamp -skipObf"
}
