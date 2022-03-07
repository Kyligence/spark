def call(String version, boolean skipPatch = true, boolean skipLicense = true) {
    sh "git apply license.patch"
    sh """
        wget http://mvn.ops.kylincorp.com/repository/raw-tars-hosted/io.kyligence.ke/grafana-6.2.4.linux-amd64.tar.gz --directory-prefix=build/
        wget http://mvn.ops.kylincorp.com/repository/raw-tars-hosted/io.kyligence.ke/influxdb-1.6.4.x86_64.rpm --directory-prefix=build/
        wget http://mvn.ops.kylincorp.com/repository/raw-tars-hosted/io.kyligence.spark/spark-newten-3.2.0-4.x-r56.tgz --directory-prefix=build/

        wget http://mvn.ops.kylincorp.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-10.7-1PGDG.rhel6.x86_64.rpm --directory-prefix=build/postgresql/
        wget http://mvn.ops.kylincorp.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-libs-10.7-1PGDG.rhel6.x86_64.rpm --directory-prefix=build/postgresql/
        wget http://mvn.ops.kylincorp.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-server-10.7-1PGDG.rhel6.x86_64.rpm --directory-prefix=build/postgresql/

        wget http://mvn.ops.kylincorp.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-10.7-1PGDG.rhel7.x86_64.rpm --directory-prefix=build/postgresql/
        wget http://mvn.ops.kylincorp.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-libs-10.7-1PGDG.rhel7.x86_64.rpm --directory-prefix=build/postgresql/
        wget http://mvn.ops.kylincorp.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-server-10.7-1PGDG.rhel7.x86_64.rpm --directory-prefix=build/postgresql/

        wget http://mvn.ops.kylincorp.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-10.8-1PGDG.rhel8.x86_64.rpm --directory-prefix=build/postgresql/
        wget http://mvn.ops.kylincorp.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-libs-10.8-1PGDG.rhel8.x86_64.rpm --directory-prefix=build/postgresql/
        wget http://mvn.ops.kylincorp.com/repository/raw-tars-hosted/io.kyligence.ke/postgresql10-server-10.8-1PGDG.rhel8.x86_64.rpm --directory-prefix=build/postgresql/
    """
    sh "export release_version=${version} && sh build/script_newten/release.sh -noTimestamp"
}
