package io.kyligence.devops.jenkins.etl

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import groovy.util.logging.Slf4j
import io.kyligence.devops.jenkins.client.JenkinsConfig

@Slf4j
class NewtenBasicPostAnalyzer extends AbstractPostAnalyzer {


    NewtenBasicPostAnalyzer(JenkinsConfig config, AmazonS3 s3client) {
        super(config, s3client)
    }

    void process(JenkinsTransform.Csv data) {
        log.info("processing basic.csv...")
        final JenkinsTransform.Csv result = new JenkinsTransform.Csv()

        data.getRows().forEach(row -> {
            def r = new ArrayList<JenkinsAnalysis.Column>()
            row.forEach(col -> {
                if (col.getName().endsWith("TestState") || col.getName().endsWith("TestDuration")) {
                    return
                }

                r.add(col)
            })

            result.addRow(r)
        })


        def dataStream = result.toStream()
        def meta = new ObjectMetadata()
        meta.setContentLength(dataStream.available())
        s3client.putObject(config.getS3StoreBucket(), "${String.format(DATA_KEY_PREFIX, config.getPlatform(), "KE4", "Newten_CI_On_GCP")}/basic/data.csv", dataStream, meta)
    }
}
