package io.kyligence.devops.jenkins.etl

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import groovy.util.logging.Slf4j
import io.kyligence.devops.jenkins.client.JenkinsConfig

@Slf4j
class ClickhouseItBasicPostAnalyzer extends AbstractPostAnalyzer {


    ClickhouseItBasicPostAnalyzer(JenkinsConfig config, AmazonS3 s3client) {
        super(config, s3client)
    }

    void process(JenkinsTransform.Csv data) {
        log.info("processing basic.csv...")
        def dataStream = data.toStream()
        def meta = new ObjectMetadata()
        meta.setContentLength(dataStream.available())
        s3client.putObject(config.getS3StoreBucket(), "${String.format(DATA_KEY_PREFIX, config.getPlatform(), "KE4", "Clickhouse-IT-Only")}/basic/data.csv", dataStream, meta)
    }
}
