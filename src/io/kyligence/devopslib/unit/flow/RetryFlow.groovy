package io.kyligence.devopslib.unit.flow

import io.kyligence.devopslib.Utils
import io.kyligence.devopslib.unit.Unit
import io.netty.util.internal.ThrowableUtil
import org.jenkinsci.plugins.workflow.steps.FlowInterruptedException

class RetryFlow<I, O> {

    private final Unit<I, O> unit

    RetryFlow(Unit<I, O> unit) {
        this.unit = unit
    }

    O execute(I input, boolean failFast, int maxRetries, long retryDelay) {
        int retries = 0
        while (true) {
            try {
                unit.run(input)
            } catch (Exception ex) {
                if (failFast || ex instanceof FlowInterruptedException) {
                    Utils.log("execute unit[${unit.getName()}] failfast!")
                    throw ex
                }

                Utils.log("execute unit[${unit.getName()}] error: ${ex.getMessage()}")
                Utils.log("${ThrowableUtil.stackTraceToString(ex)}")
            }

            if (retries >= maxRetries) {
                throw new RuntimeException("Failed unit[${unit.getName()}] after " + retries + " retries.")
            }

            if (unit.isSuccess()) {
                return unit.getResult()
            }

            Utils.log("Failed, retrying unit[${unit.getName()}] in " + retryDelay + "ms...")
            Thread.sleep(retryDelay)
            retries++
        }
    }
}
