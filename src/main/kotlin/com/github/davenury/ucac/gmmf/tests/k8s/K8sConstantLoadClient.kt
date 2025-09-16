package com.github.davenury.ucac.gmmf.tests.k8s

import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.apis.BatchV1Api
import io.kubernetes.client.openapi.models.V1ConfigMapEnvSource
import io.kubernetes.client.openapi.models.V1Container
import io.kubernetes.client.openapi.models.V1ContainerBuilder
import io.kubernetes.client.openapi.models.V1EnvFromSource
import io.kubernetes.client.openapi.models.V1Job
import io.kubernetes.client.openapi.models.V1JobBuilder
import io.kubernetes.client.openapi.models.V1Volume
import io.kubernetes.client.openapi.models.V1VolumeBuilder
import org.slf4j.LoggerFactory

/**
 * @author Kamil Jarosz
 */
class K8sConstantLoadClient(
    private val configMapName: String,
    private val image: String?,
    namespace: String,
    private val graphPath: String,
    private val constantLoadOpts: String,
    resourceCpu: String,
    resourceMemory: String,
) {
    private val graphPvc: K8sGraphPvc
    private val namespace: String
    private val graphName = "constant-client-graph"
    private val jobName = "constant-load"
    private val volumeName = "graph-from-cm"
    private val resourceCpu: String
    private val resourceMemory: String

    init {
        this.graphPvc = K8sGraphPvc(namespace, graphName)
        this.namespace = namespace
        this.resourceCpu = resourceCpu
        this.resourceMemory = resourceMemory
    }

    private fun buildJob(old: V1Job): V1Job {
        return V1JobBuilder(old)
            .editOrNewMetadata()
            .withName(jobName)
            .endMetadata()
            .editOrNewSpec()
            .editOrNewTemplate()
            .editOrNewSpec()
            .withVolumes(buildVolume())
            .withContainers(buildContainer())
            .withRestartPolicy("Never")
            .endSpec()
            .endTemplate()
            .withBackoffLimit(0)
            .endSpec()
            .build()
    }

    private fun buildContainer(): V1Container {
        return V1ContainerBuilder()
            .withName("constant-load-client")
            .withImage(image ?: K8sPeer.DEFAULT_IMAGE)
            .withImagePullPolicy("Always")
            .withArgs("constant-load", "-g", "/graph/$graphPath")
            .addToArgs(*constantLoadOpts.split("\\s+".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray())
            .addNewVolumeMount()
            .withName(volumeName)
            .withNewMountPath("/graph")
            .endVolumeMount()
            .addNewEnv()
            .withName("CONFIG_FILE")
            .withValue("application-kubernetes-test-client.conf")
            .endEnv()
            .withEnvFrom(
                V1EnvFromSource()
                    .configMapRef(
                        V1ConfigMapEnvSource()
                            .name(configMapName),
                    ),
            )
            .editOrNewResources()
            .addToRequests("cpu", Quantity.fromString(resourceCpu))
            .addToRequests("memory", Quantity.fromString(resourceMemory))
            .endResources()
            .build()
    }

    private fun buildVolume(): V1Volume {
        return V1VolumeBuilder()
            .withName(volumeName)
            .withNewPersistentVolumeClaim()
            .withClaimName(graphName)
            .endPersistentVolumeClaim()
            .build()
    }

    @Throws(ApiException::class)
    fun apply() {
        logger.info("Applying constant load client")
        graphPvc.apply()

        if (exists()) {
            logger.info("  Deleting the existing constant load client...")
            batchApi.deleteNamespacedJob(jobName, namespace, null, null, 0, null, null, null)

            while (exists()) {
                try {
                    Thread.sleep(200)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw RuntimeException(e)
                }
            }
        }

        logger.info("  Creating a constant load client...")
        createJob()
    }

    @Throws(ApiException::class)
    fun createJob() {
        val job = buildJob(V1Job())
        batchApi.createNamespacedJob(namespace, job, null, null, null)
    }

    @Throws(ApiException::class)
    fun exists(): Boolean {
        try {
            batchApi.readNamespacedJob(jobName, namespace, null, null, null)
            return true
        } catch (e: ApiException) {
            if (e.code == 404) {
                return false
            }

            throw e
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("k8s-peer")
        private val batchApi = BatchV1Api()
    }
}
