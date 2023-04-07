package io.kyligence.devopslib.unit

import io.kyligence.devopslib.Utils

abstract class Unit<I, O> implements Serializable {

    final String name

    final String unitspace

    O result

    Unit(String name) {
        this.name = name
        this.unitspace = Utils.ctx.pwd()
    }

    abstract void run(I input)

    abstract boolean isSuccess()
}
