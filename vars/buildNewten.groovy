def call(String version, boolean skipPatch = true, boolean skipLicense = true) {
    if (!skipPatch) {
        // sh 'git apply /var/cicd/patchs/newten.patch'
        sh """
        echo "replace patch"

        rm -rf /var/cicd/newten.patch
        cp -rf license.patch /var/cicd/patchs/newten.patch
        git apply /var/cicd/patchs/newten.patch
        """

// TODO: hasPatch
/* if [ "$hasPatch" == "false" ]; then
    echo "skip apply patch for Kyligence Enterprise 4.x ."
    exit 0;
fi

if [ -f license.patch ]; then
    git apply license.patch || 1
elif [ -f "/mnt/jenkins/kap/master/license/${branch}.patch" ]; then
    git apply "/mnt/jenkins/kap/master/license/${branch}.patch" || 1
elif [[ ${branch} == "ke-4.1."* ]]
then
    git apply "/mnt/jenkins/kap/master/license/ke-4.1.x.patch" || 1
elif [[ ${branch} == "ke-4.2."* ]]
then
    git apply "/mnt/jenkins/kap/master/license/ke-4.2.x.patch" || 1
elif [[ ${branch} == "ke-4.3."* ]]
then
    git apply "/mnt/jenkins/kap/master/license/ke-4.3.x-qa.patch" || 1
elif [[ ${branch} == *"monikor-4.2."* ]]
then
    git apply "/mnt/jenkins/kap/master/license/ke-4.2.x.patch" || 1
else 
    git apply /mnt/jenkins/kap/master/license/newten.patch || 1
fi
 */
    }

    if (!skipLicense) {
        sh script:
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

// TODO: hasLicense
/* if [ "$hasLicense" == "false" ]; then
    echo "skip create license for Kyligence Enterprise 4.x ."
    exit 0;
fi

# generate license for kap
cat << EOF > trial-license.cfg
License for Kyligence Enterprise Evaluation.
Category:4.x
Volume:10
Service Nodes:5
====
version=Kyligence Enterprise ${release_version/-/ }
EOF

kap_commit_sha1=`git rev-parse HEAD`
exp_date=`date -d "+1 month" +%Y-%m-%d`

echo "kap-commit=${kap_commit_sha1}" >> trial-license.cfg
echo "exp-date=${exp_date}" >> trial-license.cfg

curl -H "Authorization: Basic amVua2luczpLeUJvdElzQmVzdDIwMTY=" -F cfg=@trial-license.cfg http://priv-lic5042.chinacloudapp.cn:18086/api/license/kap -o LICENSE

if [ -z `grep -l "====" LICENSE` ]; then 
    echo "license not created."
    exit 1; 
fi */

    }
    sh "npm config set registry http://mvn.ops.kylincorp.com/repository/npm-public/"

    sh "export release_version=${version} && sh build/script_newten/release.sh -noTimestamp"
}
