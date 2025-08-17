package com.github.davenury.ucac.gmmf.tests.cli

import com.github.davenury.ucac.Config
import com.github.davenury.ucac.gmmf.tests.RemoteGraphBuilder
import com.github.kjarosh.agh.pp.graph.GraphLoader
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Options
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

object ConstantLoadClientMain {
    private val logger = LoggerFactory.getLogger("constant-load")

    @JvmStatic
    fun main(
        config: Config,
        args: Array<String>,
    ) {
        val options = Options()
        options.addRequiredOption("l", "load-graph", false, "load graph at the beginning")
        options.addRequiredOption("g", "graph", true, "path to graph")
        options.addRequiredOption("n", "operations", true, "number of operations per second")
        options.addOption(null, "threads", true, "number of threads")

        val parser: CommandLineParser = DefaultParser()
        val cmd = parser.parse(options, args)

        val peerResolver = config.newPeerResolver()

        if (cmd.hasOption("l")) {
            val graphPath = cmd.getOptionValue("g")
            logger.info("Loading graph {}", graphPath)
            val graph = GraphLoader.loadGraph(graphPath)
            RemoteGraphBuilder(graph, peerResolver).build()
        }

        if (cmd.hasOption("n")) {
            val threads = Integer.parseInt(cmd.getOptionValue("threads", "4"))
            val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(threads)

            val opsPerSecond = Integer.parseInt(cmd.getOptionValue("n"))
            logger.info("Running {} operations per second", opsPerSecond)

            val periodSeconds = 1.0 / opsPerSecond.toDouble()
            val periodNanoseconds = periodSeconds * 1_000_000_000.0
            scheduler.scheduleAtFixedRate({ this.performOperation() }, 0, periodNanoseconds.toLong(), TimeUnit.NANOSECONDS)

            scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)
        }
    }

    fun performOperation() {
        logger.info("Beep bop")
    }
}
