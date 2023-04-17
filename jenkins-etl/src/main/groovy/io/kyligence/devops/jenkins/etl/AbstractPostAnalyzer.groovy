package io.kyligence.devops.jenkins.etl

import com.amazonaws.services.s3.AmazonS3
import io.kyligence.devops.jenkins.client.JenkinsConfig
import io.kyligence.devops.jenkins.etl.JenkinsAnalysis.Column
import io.kyligence.devops.jenkins.etl.JenkinsTransform.Csv

abstract class AbstractPostAnalyzer {

    protected static final String CSV_PREFIX = "csv/%s"
    protected static final String DATA_KEY_PREFIX = CSV_PREFIX + "/%s/%s"

    protected final AmazonS3 s3client

    protected final JenkinsConfig config

    AbstractPostAnalyzer(JenkinsConfig config, AmazonS3 s3client) {
        this.config = config
        this.s3client = s3client
    }

    protected Column findColumnByName(List<Column> row, String name) {
        return row.get(row.indexOf(Column.valueOf(name, null)))
    }


    protected String compatiblePlatform(final String platform) {
        if (this.config.isCompatibleGCP() && platform.equals("aws-mixed")) {
            return "gcp"
        }

        return platform
    }

    abstract void process(Csv data);
}
