def call(String version, boolean noSpark = false, String docs_commitid = 'latest', String customerPkg = 'NORMAL') {
    sh "git apply license.patch"
    sh """
        wget http://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/grafana-6.2.4.linux-amd64.tar.gz --directory-prefix=build/
        wget http://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/hive_1_2_2.tar.gz --directory-prefix=build/

        wget http://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/influxdb-1.6.4.x86_64.rpm --directory-prefix=build/

        wget http://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-10.7-1PGDG.rhel6.x86_64.rpm --directory-prefix=build/postgresql/
        wget http://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-libs-10.7-1PGDG.rhel6.x86_64.rpm --directory-prefix=build/postgresql/
        wget http://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-server-10.7-1PGDG.rhel6.x86_64.rpm --directory-prefix=build/postgresql/

        wget http://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-10.7-1PGDG.rhel7.x86_64.rpm --directory-prefix=build/postgresql/
        wget http://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-libs-10.7-1PGDG.rhel7.x86_64.rpm --directory-prefix=build/postgresql/
        wget http://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-server-10.7-1PGDG.rhel7.x86_64.rpm --directory-prefix=build/postgresql/

        wget http://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-10.8-1PGDG.rhel8.x86_64.rpm --directory-prefix=build/postgresql/
        wget http://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-libs-10.8-1PGDG.rhel8.x86_64.rpm --directory-prefix=build/postgresql/
        wget http://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-server-10.8-1PGDG.rhel8.x86_64.rpm --directory-prefix=build/postgresql/
    """
    sh "npm cache verify"

    if (noSpark) {
        echo "Package not include Spark"
        sh "export ${customerPkg}=1 && export release_version=${version} && sh build/script_newten/release.sh -noTimestamp -noSpark"
    } else {
        insertDocs(version, docs_commitid)
        sh "export ${customerPkg}=1 && export release_version=${version} && sh build/script_newten/release.sh -noTimestamp"
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
            wget http://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/docs/${docs_version}/${docs_commitid}/en/Kyligence_Enterprise_User_Manual-en.pdf --directory-prefix=build/docs
            wget http://repo-ofs.kyligence.com/repository/raw-tars-hosted/io.kyligence.ke/docs/${docs_version}/${docs_commitid}/zh-cn/Kyligence_Enterprise_User_Manual-zh.pdf --directory-prefix=build/docs
        """

        sh "ls -lh build/docs"
    }
}