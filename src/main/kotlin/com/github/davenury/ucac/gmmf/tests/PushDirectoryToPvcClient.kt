package com.github.davenury.ucac.gmmf.tests

import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1PodBuilder
import io.kubernetes.client.util.Config
import io.kubernetes.client.util.KubeConfig
import java.io.InputStreamReader
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.commons.cli.CommandLineParser
import org.apache.commons.cli.DefaultParser
import org.apache.commons.cli.Options
import org.apache.commons.cli.ParseException

/**
 * @author Kamil Jarosz
 */
object PushDirectoryToPvcClient {
    @Throws(ParseException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        println(args.contentToString())
        val options = Options()
        options.addOption("c", "config", true, "path to kubectl config")
        options.addOption("n", "namespace", true, "namespace to use")
        options.addOption(null, "dir", true, "directory to push")
        options.addOption(null, "pvc", true, "pvc name")

        val parser: CommandLineParser = DefaultParser()
        val cmd = parser.parse(options, args)

        val dir = cmd.getOptionValue("dir")
        val pvc = cmd.getOptionValue("pvc")
        val namespace = cmd.getOptionValue("n") ?: throw RuntimeException("No namespace")

        setupConfig(cmd.getOptionValue("c"))

        setupPod(namespace, pvc)
        waitForPod(namespace)
        copyDirectoryToPod(namespace, dir)
        tearDownPod(namespace)
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
    private fun setupPod(
        namespace: String,
        pvc: String,
    ) {
        val pod =
            V1PodBuilder()
                .withNewMetadata()
                .withName("pvc-helper")
                .addToLabels("app", "pvc-helper")
                .endMetadata()
                .withNewSpec()
                .addNewContainer()
                .withName("helper")
                .withImage("bash")
                .withCommand("sleep")
                .withArgs("3600")
                .addNewVolumeMount()
                .withName("pvc")
                .withMountPath("/data")
                .endVolumeMount()
                .endContainer()
                .addNewVolume()
                .withName("pvc")
                .withNewPersistentVolumeClaim()
                .withClaimName(pvc)
                .endPersistentVolumeClaim()
                .endVolume()
                .withRestartPolicy("Never")
                .endSpec()
                .build()

        CoreV1Api().createNamespacedPod(namespace, pod, null, null, null)
    }

    private fun waitForPod(namespace: String) {
        while (true) {
            Thread.sleep(500)
            val pod = CoreV1Api().readNamespacedPod("pvc-helper", namespace, null, null, null)
            if (pod.status?.phase == "Running") {
                println("Pod is running")
                return
            }
        }
    }

    private fun copyDirectoryToPod(
        namespace: String,
        dir: String,
    ) {
        val processBuilder = ProcessBuilder("kubectl", "cp", "$dir/", "$namespace/pvc-helper:/data")
        println("Running " + processBuilder.command())
        processBuilder.redirectErrorStream(true)
        val process = processBuilder.start()

        val output = process.inputStream.bufferedReader().readText()
        val status = process.waitFor()

        println("Finished copying:\n$output")

        if (status != 0) {
            throw RuntimeException("Error during copying: $status")
        }
    }

    @Throws(ApiException::class)
    private fun tearDownPod(namespace: String) {
        CoreV1Api().deleteNamespacedPod("pvc-helper", namespace, null, null, null, null, null, null)
    }
}
