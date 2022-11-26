package io.kyligence.devops.jenkins.etl

import com.amazonaws.services.s3.model.S3Object
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.jayway.jsonpath.Configuration
import com.jayway.jsonpath.DocumentContext
import com.jayway.jsonpath.JsonPath
import com.jayway.jsonpath.Option
import com.jayway.jsonpath.spi.json.JacksonJsonNodeJsonProvider
import groovy.transform.EqualsAndHashCode

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
        public String toString() {
            return "${name}[${value}]"
        }
    }

    private static class Evaluate {

        private ObjectMapper objectMapper = new ObjectMapper()

        private DocumentContext docCtx

        Evaluate(DocumentContext docCtx) {
            this.docCtx = docCtx
        }

        ArrayNode length(String jsonpath) {
            final ArrayNode colValue = docCtx.read(jsonpath)
            return objectMapper.createArrayNode().add(colValue.size())
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
            def objContent = objectMapper.readTree(obj.getObjectContent())

            def closure = (Map.Entry<String, String> it) -> {
                def docCtx = JsonPath.using(JSONPATH_CONF).parse(objContent)

                def colName = it.getKey()
                def valueExpression = it.getValue()

                final ArrayNode colValue = valueExpression.startsWith("\$") ?
                        docCtx.<ArrayNode> read(valueExpression) : (ArrayNode) Eval.x(new Evaluate(docCtx), "x.${valueExpression}")

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


        } else {
            obj.getObjectContent().abort()
        }
    }

    List<Column> analysisResult() {
        return this.columns.sort(false)
    }

}
