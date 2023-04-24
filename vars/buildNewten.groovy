def call(String version, boolean noSpark = false, String docs_commitid = 'latest', String customerPkg = 'NORMAL', boolean excludeDocs = false) {
    try{
        sh "git apply license.patch"
    } catch(ex){
        println "git apply fail: ${ex.toString()}"
    }
    // sh "pwd && ls -lha"
    sh """
        wget https://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/grafana-6.2.4.linux-amd64.tar.gz --directory-prefix=build/ --no-check-certificate
        wget https://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/hive_1_2_2.tar.gz --directory-prefix=build/ --no-check-certificate

        wget https://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/influxdb-1.6.4.x86_64.rpm --directory-prefix=build/ --no-check-certificate

        wget https://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-10.7-1PGDG.rhel6.x86_64.rpm --directory-prefix=build/postgresql/ --no-check-certificate
        wget https://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-libs-10.7-1PGDG.rhel6.x86_64.rpm --directory-prefix=build/postgresql/ --no-check-certificate
        wget https://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-server-10.7-1PGDG.rhel6.x86_64.rpm --directory-prefix=build/postgresql/ --no-check-certificate

        wget https://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-10.7-1PGDG.rhel7.x86_64.rpm --directory-prefix=build/postgresql/ --no-check-certificate
        wget https://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-libs-10.7-1PGDG.rhel7.x86_64.rpm --directory-prefix=build/postgresql/ --no-check-certificate
        wget https://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-server-10.7-1PGDG.rhel7.x86_64.rpm --directory-prefix=build/postgresql/ --no-check-certificate

        wget https://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-10.8-1PGDG.rhel8.x86_64.rpm --directory-prefix=build/postgresql/ --no-check-certificate
        wget https://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-libs-10.8-1PGDG.rhel8.x86_64.rpm --directory-prefix=build/postgresql/ --no-check-certificate
        wget https://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-server-10.8-1PGDG.rhel8.x86_64.rpm --directory-prefix=build/postgresql/ --no-check-certificate
    """
    dir('kystudio') {
        sh """
            pwd && ls -lha
            npm cache verify
            node -v
            npm -v
            npm install npm -g
            node -v
            npm -v
            npm cache clean --force
            npm rebuild
            node -v
            npm -v
        """
        retry(3){
            sh "npm install --registry https://repo-ofs.kyligence.com/repository/npm-public/"
        }
    }
    if (noSpark) {
        echo "Package not include Spark"
        retry (2) {
            sh "export ${customerPkg}=1 && export release_version=${version} && npm config set strict-ssl=false && sh build/script_newten/release.sh -noTimestamp -noSpark -Dmaven.wagon.http.ssl.insecure=true -U"
        }
    } else {
        if (!excludeDocs) {
            insertDocs(version, docs_commitid)
        }

        retry (2) {
            sh "export ${customerPkg}=1 && export release_version=${version} && npm config set strict-ssl=false && sh build/script_newten/release.sh -noTimestamp -Dmaven.wagon.http.ssl.insecure=true -U"
        }
    }
}

def insertDocs(version, docs_commitid) {
    def version_splits = version.split("\\.")
    if (version_splits.length < 3) {
        throw new IllegalArgumentException("version must like major.minor.revision, but is ${version}")
    }
    def docs_version = "v${version_splits[0]}.${version_splits[1]}"

    if (docs_commitid.isEmpty()) {
        throw new IllegalArgumentException("docs commit id not allowed to be empty")
    }

    println("docs_version: ${docs_version}")
    println("docs_commitid: ${docs_commitid}")

    if (docs_version >= 'v4.5') {
        println("downloading docs...")
        sh """
            wget https://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/docs/${docs_version}/${docs_commitid}/en/Kyligence_Enterprise_User_Manual-en.pdf --directory-prefix=build/docs --no-check-certificate
            wget https://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/docs/${docs_version}/${docs_commitid}/zh-cn/Kyligence_Enterprise_User_Manual-zh.pdf --directory-prefix=build/docs --no-check-certificate
        """

        sh "ls -lh build/docs"
    }
}