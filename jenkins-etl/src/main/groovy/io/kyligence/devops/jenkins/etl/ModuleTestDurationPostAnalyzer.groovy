package io.kyligence.devops.jenkins.etl

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import groovy.util.logging.Slf4j
import io.kyligence.devops.jenkins.client.JenkinsConfig
import io.kyligence.devops.jenkins.etl.JenkinsTransform.Csv

@Slf4j
class ModuleTestDurationPostAnalyzer extends AbstractPostAnalyzer {

    private final static SUFFIX = "TestDuration"


    ModuleTestDurationPostAnalyzer(JenkinsConfig config, AmazonS3 s3client) {
        super(config, s3client)
    }

    void process(Csv data) {
        log.info("processing module_test_durations.csv...")
        final Csv result = new Csv()

        data.getRows().forEach(row -> {
            if (findColumnByName(row, "result")?.getValue() == "FAILURE") {
                return
            }

            def idCol = findColumnByName(row, "id")
            def runDateCol = findColumnByName(row, "runDate")

            row.forEach(col -> {
                if (!col.getName().endsWith(SUFFIX)) {
                    return
                }

                result.addRow([idCol, runDateCol, JenkinsAnalysis.Column.valueOf("moduleName", col.getName().replace(SUFFIX, "")), JenkinsAnalysis.Column.valueOf("testDuration", col.getValue())])
            })
        })


        def dataStream = result.toStream()
        def meta = new ObjectMetadata()
        meta.setContentLength(dataStream.available())
        s3client.putObject(config.getS3StoreBucket(), "${String.format(DATA_KEY_PREFIX, config.getPlatform(), "KE4", "Newten_CI_On_GCP")}/module_test_durations/data.csv", dataStream, meta)
    }
}
