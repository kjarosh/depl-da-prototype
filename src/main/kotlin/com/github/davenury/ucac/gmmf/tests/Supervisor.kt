package com.github.davenury.ucac.gmmf.tests

import java.time.Duration
import java.util.function.DoubleSupplier

/**
 * @author Kamil Jarosz
 */
class Supervisor(
    private val verticesBuilt: DoubleSupplier?,
    private val edgesBuilt: DoubleSupplier?,
) : Thread() {
    private var delay: Duration = Duration.ofMillis(1000)

    fun setDelay(delay: Duration) {
        this.delay = delay
    }

    override fun run() {
        while (!interrupted()) {
            try {
                sleep(delay.toMillis())
            } catch (e: InterruptedException) {
                return
            }

            val message = StringBuilder()
            if (verticesBuilt != null) {
                val v = verticesBuilt.asDouble
                message.append(String.format("V: %.2f%%    ", v * 100))
            }

            if (edgesBuilt != null) {
                val e = edgesBuilt.asDouble
                message.append(String.format("E: %.2f%%    ", e * 100))
            }

            println(message)
        }
    }
}
