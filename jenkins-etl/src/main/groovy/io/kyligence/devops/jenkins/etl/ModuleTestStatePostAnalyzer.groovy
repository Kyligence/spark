package io.kyligence.devops.jenkins.etl

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import groovy.util.logging.Slf4j
import io.kyligence.devops.jenkins.client.JenkinsConfig
import io.kyligence.devops.jenkins.etl.JenkinsTransform.Csv

@Slf4j
class ModuleTestStatePostAnalyzer extends AbstractPostAnalyzer {

    private final static SUFFIX = "TestState"


    ModuleTestStatePostAnalyzer(JenkinsConfig config, AmazonS3 s3client) {
        super(config, s3client)
    }

    void process(Csv data) {
        log.info("processing module_test_states.csv...")
        final Csv result = new Csv()

        data.getRows().forEach(row -> {
            if (findColumnByName(row, "result")?.getValue() != "FAILURE") {
                return
            }

            if (findColumnByName(row, "testingFailed")?.getValue() != "1") {
                return
            }

            def idCol = findColumnByName(row, "id")

            row.forEach(col -> {
                if (!col.getName().endsWith(SUFFIX)) {
                    return
                }

                result.addRow([idCol, JenkinsAnalysis.Column.valueOf("moduleName", col.getName().replace(SUFFIX, "")), JenkinsAnalysis.Column.valueOf("testState", col.getValue())])
            })
        })


        def dataStream = result.toStream()
        def meta = new ObjectMetadata()
        meta.setContentLength(dataStream.available())
        s3client.putObject(config.getS3StoreBucket(), "${String.format(DATA_KEY_PREFIX, config.getPlatform(), "KE4", "Newten_CI_On_GCP")}/module_test_states/data.csv", dataStream, meta)
    }
}
