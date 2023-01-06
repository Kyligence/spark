package io.kyligence.devops.jenkins.etl

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.S3ObjectSummary
import groovy.util.logging.Slf4j
import io.kyligence.devops.jenkins.client.JenkinsConfig
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets
import java.util.stream.Collectors

@Slf4j
class JenkinsTransform {

    static class Csv {
        private List<List<JenkinsAnalysis.Column>> rows = new ArrayList<>()

        private List<String> header

        synchronized void addRow(List<JenkinsAnalysis.Column> row) {
            if (header == null) {
                header = row.collect {
                    it.getName()
                }
            }

            log.info("add row: ${row}")

            if (row.size() != header.size()) {
                throw new IllegalStateException("header size: ${header.size()}, row size: ${row.size()}")
            }

            rows.add(row)
        }

        synchronized InputStream toStream() {
            def builder = new StringBuilder()

            builder.append(header.stream().collect(Collectors.joining(","))).append("\n")

            log.info("total rows: ${rows.size()}")
            for (row in rows) {
                builder.append(row.stream().map(r -> r.getValue()).collect(Collectors.joining(","))).append("\n")
            }

            return IOUtils.toInputStream(builder, StandardCharsets.UTF_8)
        }

        synchronized List<List<JenkinsAnalysis.Column>> getRows() {
            return new ArrayList<List<JenkinsAnalysis.Column>>(this.rows)
        }
    }

    private static final String ROOT_PREFIX = "original/%s"
    private static final String JOB_KEY_PREFIX = ROOT_PREFIX + "/%s/%s"

    private static final String CSV_PREFIX = "csv/%s"
    private static final String DATA_KEY_PREFIX = CSV_PREFIX + "/%s/%s"

    private JenkinsConfig config

    private String storeBucket

    private AmazonS3 s3client

    private final Csv csv

    private final List<AbstractPostAnalyzer> postAnalyzers = new ArrayList<>()

    JenkinsTransform(JenkinsConfig config) {
        this(config, new ArrayList<Class<? extends AbstractPostAnalyzer>>())
    }

    JenkinsTransform(JenkinsConfig config, List<Class<? extends AbstractPostAnalyzer>> postAnalyzerClasses) {
        this.config = config
        this.storeBucket = config.getS3StoreBucket()

        this.s3client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(config.getS3Accesskey(), config.getS3secretKey())))
                .withRegion(Regions.CN_NORTH_1)
                .build();

        this.csv = new Csv()

        postAnalyzerClasses.forEach(it -> {
            this.postAnalyzers.add(it.newInstance(this.config, s3client))
        })


    }

    void execute(String jobFolder, String jobName) {
        def jobs = listFolders(String.format(JOB_KEY_PREFIX, config.getPlatform(), jobFolder, jobName))
        log.info("total jobs: ${jobs.size()}")

        jobs.parallelStream().forEach(job -> {
            def objs = listObjects(job)
            if (objs.stream().noneMatch(it -> it.getKey().endsWith(".flag"))) {
                log.info("job [${job}] unfinished, skip it.")
                return
            }

            log.info("transform job: [${job}]...")
            def analysis = new JenkinsAnalysis(jobFolder, jobName)

            // .flag -> console.log -> run.json -> step_logs/* -> test.json
            objs.forEach {
                try (def obj = s3client.getObject(config.getS3StoreBucket(), it.getKey())) {
                    analysis.analyze(obj)
                }
            }

            csv.addRow(analysis.analysisResult())
        })

        def dataStream = csv.toStream()
        def meta = new ObjectMetadata()
        meta.setContentLength(dataStream.available())
        s3client.putObject(config.getS3StoreBucket(), "${String.format(DATA_KEY_PREFIX, config.getPlatform(), jobFolder, jobName)}/data.csv", dataStream, meta)

        postAnalyzers.forEach(it -> {
            it.process(csv)
        })

    }

    private List<String> listFolders(String prefix) {
        def folders = new ArrayList<String>()

        def req = new ListObjectsRequest()
        req.setBucketName(config.getS3StoreBucket())
        req.setPrefix(prefix.endsWith("/") ? prefix : prefix + "/")
        req.setDelimiter("/")

        def resp = s3client.listObjects(req)
        folders.addAll(resp.getCommonPrefixes())

        while (resp.isTruncated()) {
            resp = s3client.listNextBatchOfObjects(resp)
            folders.addAll(resp.getCommonPrefixes())
        }

        return folders.reverse()
    }

    private List<S3ObjectSummary> listObjects(String folder) {
        def objs = []

        def req = new ListObjectsRequest()
        req.setBucketName(config.getS3StoreBucket())
        req.setPrefix(folder)

        def resp = s3client.listObjects(req)
        objs.addAll(resp.getObjectSummaries())

        while (resp.isTruncated()) {
            resp = s3client.listNextBatchOfObjects(resp)
            objs.addAll(resp.getObjectSummaries())
        }

        return objs
    }
}
