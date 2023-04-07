package io.kyligence.devopslib.unit

import groovy.transform.builder.Builder
import io.kyligence.devopslib.Utils
import io.kyligence.devopslib.unit.flow.ParallelFlow
import io.kyligence.devopslib.unit.flow.TestFlow

import java.util.concurrent.LinkedBlockingQueue

class NewtenDynamicParallelFlow {

    final int MAX_UTEST_WORKERS = 2
    final int MAX_ITEST_WORKERS = 3

    final Map<String, String> jobEnv

    NewtenDynamicParallelFlow(Map<String, String> jobEnv) {
        this.jobEnv = jobEnv
    }

    @Builder
    static class DynamicParallelFlowParameter {
        String orgRepo
        String ghprbPullId
        boolean failFast
        boolean skipBuild

        boolean isDaily
    }

    def execute(DynamicParallelFlowParameter parameter) {
        def module2Test = new NewtenModule2Test("获取待测试 Maven 模块")
        Utils.ctx.dir('sourcecode') {
            module2Test.run(NewtenModule2Test.Module2TestParameter.builder()
                    .ghprbPullId(parameter.ghprbPullId)
                    .fullRepo(parameter.orgRepo)
                    .githubCre(jobEnv.GITHUB_CRE)
                    .isDaily(parameter.isDaily)
                    .build())
        }


        if (!module2Test.isSuccess()) {
            throw new RuntimeException("获取待测试 Maven 模块异常，退出")
        }

        def testFlows = new ArrayList<TestFlow>()

        def namedModuleLists = module2Test.getResult()
        for (namedModuleList in namedModuleLists) {
            def key = namedModuleList.getName()
            def taskQueue = new LinkedBlockingQueue(namedModuleList.getModules())

            if (taskQueue.size() < 1) {
                continue
            }

            def parallelCount = { if (key == 'UTest') MAX_UTEST_WORKERS else MAX_ITEST_WORKERS }()

            if (taskQueue.size() < parallelCount) {
                parallelCount = 1
            }

            for (int i = 1; i <= parallelCount; i++) {
                testFlows.add(new TestFlow("${key}-Stage-${i}", taskQueue))
            }

        }

        def parallelFlow = ParallelFlow.from(testFlows)

        def inputs = [:]
        for (testFlow in testFlows) {
            inputs.put(testFlow.getName(), TestFlowWrapper.TestFlowWrapperParameter.builder().skipBuild(parameter.skipBuild).failFast(parameter.failFast).build())
        }

        parallelFlow.execute(parameter.failFast, inputs)
    }
}
