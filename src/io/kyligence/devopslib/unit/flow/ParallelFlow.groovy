package io.kyligence.devopslib.unit.flow

import io.kyligence.devopslib.Utils
import io.kyligence.devopslib.unit.TestFlowWrapper
import io.kyligence.devopslib.unit.Unit

class ParallelFlow<I, O> {

    private final List<Unit<I, O>> units


    static ParallelFlow from(List<TestFlow> testFlows) {
        def units = []
        for (flow in testFlows) {
            units.add(new TestFlowWrapper(flow))
        }

        return new ParallelFlow(units)
    }

    ParallelFlow(List<Unit<I, O>> units) {
        this.units = units
    }

    Map<String, O> execute(boolean failFast, Map<String, I> inputs) {
        def workers = [:]
        for (unit in units) {
            workers += createWorker(unit, inputs.get(unit.getName()))
        }

        if (failFast) {
            workers.failFast = true
        }

        Utils.log("task workers: ${workers}")

        Utils.ctx.parallel workers

        def output = new HashMap<String, O>()
        for (unit in units) {
            output.put(unit.getName(), unit.getResult())
        }

        return output
    }

    static def createWorker(Unit unit, I input) {
        def workerName = unit.getName()
        return [(workerName): {
            Utils.ctx.container('maven') {
                Utils.ctx.script {
                    unit.run(input)
                }
            }
        }]
    }
}
