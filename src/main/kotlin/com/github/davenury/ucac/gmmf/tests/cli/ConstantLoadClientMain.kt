package com.github.davenury.ucac.gmmf.tests.cli

import com.github.davenury.ucac.Config
import com.github.davenury.ucac.gmmf.tests.ClientCache
import com.github.davenury.ucac.gmmf.tests.RemoteGraphBuilder
import com.github.kjarosh.agh.pp.graph.GraphLoader
import kotlinx.coroutines.runBlocking
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

        val graphPath = cmd.getOptionValue("g")
        val graph = GraphLoader.loadGraph(graphPath)

        if (cmd.hasOption("l")) {
            logger.info("Loading graph {}", graphPath)
            RemoteGraphBuilder(graph, peerResolver).build()
        }

        val opGenerator = OperationGenerator(graph)
        val clientCache = ClientCache(peerResolver)

        if (cmd.hasOption("n")) {
            val threads = Integer.parseInt(cmd.getOptionValue("threads", "4"))
            val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(threads)

            val opsPerSecond = Integer.parseInt(cmd.getOptionValue("n"))
            logger.info("Running {} operations per second", opsPerSecond)

            val periodSeconds = 1.0 / opsPerSecond.toDouble()
            val periodNanoseconds = periodSeconds * 1_000_000_000.0
            scheduler.scheduleAtFixedRate({ this.performOperation(opGenerator, clientCache) }, 0, periodNanoseconds.toLong(), TimeUnit.NANOSECONDS)

            scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS)
        }
    }

    fun performOperation(
        opGenerator: OperationGenerator,
        clientCache: ClientCache,
    ) {
        val op = opGenerator.nextOperation()
        val e = op.edge
        if (op.delete) {
            logger.info("Deleting edge {}->{}", e.src(), e.dst())
            runBlocking {
                clientCache.getClient(e.id().to.owner()).deleteEdge(e.id())
            }
        } else {
            logger.info("Adding edge {}->{}", e.src(), e.dst())
            runBlocking {
                clientCache.getClient(e.id().to.owner()).addEdge(e.id(), e.permissions())
            }
        }
    }
}
