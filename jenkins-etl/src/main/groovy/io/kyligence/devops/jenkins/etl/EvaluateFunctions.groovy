package io.kyligence.devops.jenkins.etl

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.jayway.jsonpath.DocumentContext

class EvaluateFunctions {

    private final GroovyShell shell = new GroovyShell()

    EvaluateFunctions(DocumentContext docCtx) {
        registryFunc("length", new LengthFunc(docCtx))
        registryFunc("removeSuffix", new RemoveSuffixFunc(docCtx))
        registryFunc("replace", new ReplaceFunc(docCtx))
    }

    void registryFunc(String name, Function func) {
        shell.setVariable(name, func)
    }

    Object eval(String expression) {
        return shell.evaluate(expression)
    }

    static abstract class Function {

        protected static final ObjectMapper objectMapper = new ObjectMapper()

        protected final DocumentContext docCtx

        Function(DocumentContext docCtx) {
            this.docCtx = docCtx
        }

        ArrayNode call(String jsonpath, Object... args) {
            final ArrayNode colValue = docCtx.read(jsonpath)
            return call(colValue, args)
        }

        abstract ArrayNode call(ArrayNode val, Object... args);

    }

    static class LengthFunc extends Function {

        LengthFunc(DocumentContext docCtx) {
            super(docCtx)
        }

        @Override
        ArrayNode call(ArrayNode val, Object... args) {
            return objectMapper.createArrayNode().add(val.size())
        }
    }

    static class RemoveSuffixFunc extends Function {

        RemoveSuffixFunc(DocumentContext docCtx) {
            super(docCtx)
        }

        @Override
        ArrayNode call(ArrayNode val, Object... args) {
            return call(val, args[0] as int)
        }

        ArrayNode call(ArrayNode val, int suffixLen) {
            if (suffixLen <= 0) {
                throw new IllegalArgumentException("suffixLen <= 0")
            }

            if (val.isEmpty()) {
                return val
            }

            def v = val?.get(0)?.textValue()
            return objectMapper.createArrayNode().add(v.substring(0, v.length() - suffixLen))
        }
    }

    static class ReplaceFunc extends Function {

        ReplaceFunc(DocumentContext docCtx) {
            super(docCtx)
        }

        @Override
        ArrayNode call(ArrayNode val, Object... args) {
            return call(val, args[0] as String, args[1] as String)
        }

        ArrayNode call(ArrayNode val, String target, String replacement) {
            def v = val?.get(0)?.textValue()
            return objectMapper.createArrayNode().add(v.replace(target, replacement))
        }
    }
}
