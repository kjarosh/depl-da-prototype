package com.github.davenury.ucac.gmmf.tests

import com.github.davenury.ucac.gmmf.tests.k8s.K8sConstantLoadClient
import com.github.davenury.ucac.gmmf.tests.k8s.K8sPeer
import com.google.common.io.ByteStreams
import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.JSON
import io.kubernetes.client.util.Config
import io.kubernetes.client.util.KubeConfig
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException
import java.io.IOException
import java.io.InputStreamReader
import java.io.UncheckedIOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

/**
 * @author Kamil Jarosz
 */
object KubernetesClient {
    private var namespace: String? = null
    private var resourceCpu: String? = null
    private var resourceMemory: String? = null

    @Throws(ParseException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val options = Options()
        options.addOption("c", "config", true, "path to kubectl config")
        options.addOption("n", "namespace", true, "k8s namespace")
        options.addOption("p", "peersets", true, "peersets configuration, e.g. 3,2,6,7")
        options.addOption("g", "graph", true, "path to the graph to use")
        options.addOption("i", "image", true, "desired docker image")
        options.addOption(null, "require-cpu", true, "cpu requirement for k8s")
        options.addOption(null, "require-memory", true, "memory requirement for k8s")
        options.addOption(null, "constant-load-opts", true, "run constant load with these options")

        val parser: CommandLineParser = DefaultParser()
        val cmd = parser.parse(options, args)

        setupConfig(cmd.getOptionValue("c"))

        namespace = cmd.getOptionValue("n", null)
        if (namespace == null) {
            throw RuntimeException("No namespace")
        }
        resourceCpu = cmd.getOptionValue("require-cpu", "1")
        resourceMemory = cmd.getOptionValue("require-memory", "2Gi")

        try {
            if (cmd.hasOption("p")) {
                val peersets = cmd.getOptionValue("p").split(",").map { it.toInt() }
                setupPeers(cmd.getOptionValue("i", K8sPeer.DEFAULT_IMAGE), peersets)
            }

            if (cmd.hasOption("g") && cmd.hasOption("constant-load-opts")) {
                val constantLoadOpts = cmd.getOptionValue("constant-load-opts")
                val graphPath = Paths.get(cmd.getOptionValue("g"))
                setupConstantLoad(constantLoadOpts, graphPath)
            }
        } catch (e: ApiException) {
            processApiException(e)
        }
    }

    private fun setupConfig(configPath: String?) {
        val path =
            if (configPath != null) {
                Paths.get(configPath)
            } else {
                Paths.get(System.getProperty("user.home"))
                    .resolve(".kube/config")
            }

        Files.newInputStream(path).use { `is` ->
            InputStreamReader(`is`).use { reader ->
                val client = Config.fromConfig(KubeConfig.loadKubeConfig(reader))
                Configuration.setDefaultApiClient(client)
            }
        }
    }

    @Throws(ApiException::class)
    private fun setupPeers(
        image: String,
        peersets: List<Int>,
    ) {
        val peers = peersets.sum()
        for (p in 0 until peers) {
            K8sPeer(image, namespace!!, "peer$p", peersets, resourceCpu!!, resourceMemory!!).apply()
        }
    }

    @Throws(ApiException::class)
    private fun setupConstantLoad(
        constantLoadOpts: String,
        graphPath: Path,
    ) {
        try {
            Files.newInputStream(graphPath).use { `is` ->
                val contents: ByteArray = ByteStreams.toByteArray(`is`)
                K8sConstantLoadClient(namespace!!, contents, constantLoadOpts, resourceCpu!!, resourceMemory!!)
                    .apply()
            }
        } catch (e: IOException) {
            throw UncheckedIOException(e)
        }
    }

    private fun processApiException(e: ApiException) {
        val gson = JSON.createGson().setPrettyPrinting().create()
        System.err.println("ApiException: " + e.code + " " + e.message)
        System.err.println(gson.toJson(gson.fromJson(e.responseBody, Any::class.java)))
        e.printStackTrace()
    }
}
