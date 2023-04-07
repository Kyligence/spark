package io.kyligence.devopslib.unit

import groovy.transform.builder.Builder
import io.kyligence.devopslib.unit.flow.TestFlow

class TestFlowWrapper extends Unit<TestFlowWrapperParameter, Boolean> {

    private final TestFlow testFlow

    TestFlowWrapper(TestFlow testFlow) {
        super(testFlow.getName())
        this.testFlow = testFlow
    }

    @Builder
    static class TestFlowWrapperParameter {
        boolean skipBuild
        boolean failFast
    }

    @Override
    void run(TestFlowWrapperParameter parameter) {
        this.testFlow.execute(parameter.skipBuild, parameter.failFast)
        this.setResult(true)
    }

    @Override
    boolean isSuccess() {
        return this.getResult() != null && this.getResult()
    }
}
