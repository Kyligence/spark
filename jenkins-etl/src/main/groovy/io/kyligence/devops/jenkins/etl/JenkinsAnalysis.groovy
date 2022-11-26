package io.kyligence.devops.jenkins.etl

import com.amazonaws.services.s3.model.S3Object
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import groovy.transform.EqualsAndHashCode
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets
import java.nio.file.Paths

class JenkinsAnalysis {

    @EqualsAndHashCode
    static class Column implements Comparable<Column> {
        String name
        String value

        private Column(String name, String value) {
            this.name = name
            this.value = value ?: ""
        }

        static Column valueOf(String name, String value) {
            return new Column(name, value)
        }

        @Override
        int compareTo(Column o) {
            return this.name.compareToIgnoreCase(o.name)
        }


        @Override
        String toString() {
            return "${name}[${value}]"
        }
    }

    private static final Configuration JSONPATH_CONF = Configuration.builder().jsonProvider(new JacksonJsonNodeJsonProvider())
            .options(Option.ALWAYS_RETURN_LIST, Option.SUPPRESS_EXCEPTIONS).build()

    private final ObjectMapper objectMapper = new ObjectMapper()

    private final List<Column> columns = new ArrayList<>()

    private final String jobFolder
    private final String jobName

    private final Properties runJsonMapper = new Properties()
    private final Properties testJsonMapper = new Properties()

    JenkinsAnalysis(String jobFolder, String jobName) {
        this.jobFolder = jobFolder
        this.jobName = jobName

        runJsonMapper.load(getClass().getClassLoader().getResourceAsStream("${jobFolder}/${jobName}/run.json.mapper"))
        testJsonMapper.load(getClass().getClassLoader().getResourceAsStream("${jobFolder}/${jobName}/test.json.mapper"))
    }

    void analyze(S3Object obj) {
        def objName = Paths.get(obj.getKey()).getFileName().toString()
        if (objName.endsWith(".json")) {
            def docCtx = JsonPath.using(JSONPATH_CONF).parse(objectMapper.readTree(obj.getObjectContent()))
            def functions = new EvaluateFunctions(docCtx)

            def closure = (Map.Entry<String, String> it) -> {
                def colName = it.getKey()
                def valueExpression = it.getValue()

                final ArrayNode colValue = valueExpression.startsWith("\$") ?
                        docCtx.<ArrayNode> read(valueExpression) : (ArrayNode) functions.eval(valueExpression)

                columns.add(Column.valueOf(colName, colValue?.get(0)?.toString()))
            }

            switch (objName) {
                case "run.json":
                    runJsonMapper.entrySet().forEach(closure)
                    break
                case "test.json":
                    testJsonMapper.entrySet().forEach(closure)
                    break
                default:
                    throw new IllegalArgumentException()
            }


        } else if (objName.endsWith("console.log")) {
            def logContent = IOUtils.toString(obj.getObjectContent(), StandardCharsets.UTF_8)
            columns.add(Column.valueOf("dnsResolveFailed", logContent.contains("Could not resolve host: github.com") ? "1" : "0"))
            columns.add(Column.valueOf("k8sRuntimesFailed", logContent ==~ /io\.fabric8\.kubernetes\.client\.KubernetesClientException: not ready after [0-9]+ MILLISECONDS/ ? "1" : "0"))

        } else {
            obj.getObjectContent().abort()
        }
    }

    List<Column> analysisResult() {
        return this.columns.sort(false)
    }

}
