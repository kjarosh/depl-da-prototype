package com.github.davenury.ucac.gmmf.tests.cli

import com.github.davenury.ucac.Config
import com.github.davenury.ucac.gmmf.tests.RemoteGraphBuilder
import com.github.kjarosh.agh.pp.graph.GraphLoader
import com.zopa.ktor.opentracing.span
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Options
import org.slf4j.LoggerFactory

object ConstantLoadClientMain {
    private val logger = LoggerFactory.getLogger("OnePeersetChanges")

    @JvmStatic
    fun main(
        config: Config,
        args: Array<String>,
    ) = span("ConstantLoadClientMain.main") {
        val options = Options()
        options.addRequiredOption("l", "load-graph", false, "load graph at the beginning")
        options.addRequiredOption("g", "graph", true, "path to graph")
        options.addRequiredOption("n", "operations", true, "number of operations per second")

        val parser: CommandLineParser = DefaultParser()
        val cmd = parser.parse(options, args)

        val peerResolver = config.newPeerResolver()

        if (cmd.hasOption("l")) {
            val graphPath = cmd.getOptionValue("g")
            logger.info("Loading graph {}", graphPath)
            val graph = GraphLoader.loadGraph(graphPath)
            RemoteGraphBuilder(graph, peerResolver).build()
        }
    }
}
