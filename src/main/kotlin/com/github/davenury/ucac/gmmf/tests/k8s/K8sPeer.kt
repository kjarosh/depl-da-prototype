package com.github.davenury.ucac.gmmf.tests.k8s

import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.apis.AppsV1Api
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1Container
import io.kubernetes.client.openapi.models.V1ContainerBuilder
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1DeploymentBuilder
import io.kubernetes.client.openapi.models.V1DeploymentSpec
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimBuilder
import io.kubernetes.client.openapi.models.V1PodSpec
import io.kubernetes.client.openapi.models.V1PodTemplateSpec
import io.kubernetes.client.openapi.models.V1Service
import io.kubernetes.client.openapi.models.V1ServiceBuilder
import io.kubernetes.client.openapi.models.V1Volume
import io.kubernetes.client.openapi.models.V1VolumeBuilder
import io.kubernetes.client.openapi.models.V1VolumeMount
import io.kubernetes.client.openapi.models.V1VolumeMountBuilder
import org.slf4j.LoggerFactory
import java.util.Optional

/**
 * @author Kamil Jarosz
 */
class K8sPeer(
    image: String?,
    private val namespace: String,
    private val peerId: String,
    private val peersets: List<Int>,
    resourceCpu: String,
    resourceMemory: String,
) {
    private val image: String
    private val serviceName: String
    private val pvcName: String
    private val deploymentName: String
    private val labels: MutableMap<String, String>
    private val resourceCpu: String
    private val resourceMemory: String

    init {
        this.image = image ?: DEFAULT_IMAGE
        this.labels = HashMap()
        labels["app"] = "depl-da-peer"
        labels["peerId"] = peerId
        this.deploymentName = peerId
        this.serviceName = peerId
        this.pvcName = "$peerId-pvc"
        this.resourceCpu = resourceCpu
        this.resourceMemory = resourceMemory
    }

    private fun getPeers(): String {
        var peers = ""
        val totalPeers = peersets.sum()
        for (i in 0..<totalPeers) {
            peers += "peer$i=peer$i;"
        }
        return peers.substring(0, peers.length - 1)
    }

    private fun getPeersets(): String {
        var peers = ""
        var peerId = 0
        for ((i, p) in peersets.withIndex()) {
            peers += "peerset$i="
            for (j in 0..<p) {
                peers += "peer$peerId,"
                peerId += 1
            }
            if (peers.endsWith(",")) {
                peers = peers.substring(0, peers.length - 1)
            }
            peers += ";"
        }
        if (peers.endsWith(";")) {
            peers = peers.substring(0, peers.length - 1)
        }
        return peers
    }

    private fun buildDeployment(old: V1Deployment): V1Deployment {
        val nodeMap: MutableMap<Int, String> = HashMap()
        nodeMap[0] = "k8s-one-node-1"
        nodeMap[1] = "k8s-one-node-2"
        nodeMap[2] = "k8s-one-node-3"
        nodeMap[3] = "k8s-one-node-4"
        nodeMap[4] = "k8s-one-node-5"
        nodeMap[5] = "k8s-one-node-6"
        nodeMap[6] = "k8s-one-node-7"
        nodeMap[7] = "k8s-one-node-8"
        nodeMap[8] = "k8s-one-node-9"
        nodeMap[9] = "k8s-one-node-10"
        nodeMap[10] = "k8s-one-node-11"
        nodeMap[11] = "k8s-one-node-12"
        nodeMap[12] = "k8s-one-node-13"
        nodeMap[13] = "k8s-one-node-14"
        nodeMap[14] = "k8s-one-node-15"
        nodeMap[15] = "k8s-one-node-16"
        nodeMap[16] = "k8s-one-node-17"
        nodeMap[17] = "k8s-one-node-18"
        nodeMap[18] = "k8s-one-node-19"
        nodeMap[19] = "k8s-one-node-20"
        val nodeNumber = peerId.replace("peer".toRegex(), "").toInt()

        if (!nodeMap.containsKey(nodeNumber)) {
            throw RuntimeException("" + nodeNumber)
        }

        return V1DeploymentBuilder(old)
            .withApiVersion("apps/v1")
            .withKind("Deployment")
            .editOrNewMetadata()
            .withName(deploymentName)
            .withLabels(labels)
            .endMetadata()
            .editOrNewSpec()
            .withReplicas(1)
            .editOrNewSelector()
            .withMatchLabels(labels)
            .endSelector()
            .editOrNewTemplate()
            .editOrNewMetadata()
            .withLabels(labels)
            .endMetadata()
            .editOrNewSpec()
            .addToNodeSelector("kubernetes.io/hostname", nodeMap[nodeNumber])
            .withContainers(
                buildContainer(
                    Optional.of(old)
                        .map { obj: V1Deployment -> obj.spec }
                        .map { obj: V1DeploymentSpec? -> obj!!.template }
                        .map { obj: V1PodTemplateSpec -> obj.spec }
                        .map { obj: V1PodSpec? -> obj!!.containers }
                        .map { c: List<V1Container> -> c[0] }
                        .orElseGet { V1Container() },
                ),
            )
            .withVolumes(buildVolumes())
            .endSpec()
            .endTemplate()
            .endSpec()
            .build()
    }

    private fun buildContainer(old: V1Container): V1Container {
        return V1ContainerBuilder(old)
            .withName("zone")
            .withImage(image)
            .withArgs("server")
            .addNewEnv()
            .withName("CONFIG_FILE")
            .withValue("application-kubernetes.conf")
            .endEnv()
            .addNewEnv()
            .withName("config_port")
            .withValue("80")
            .endEnv()
            .addNewEnv()
            .withName("config_peerId")
            .withValue(peerId)
            .endEnv()
            .addNewEnv()
            .withName("config_peers")
            .withValue(getPeers())
            .endEnv()
            .addNewEnv()
            .withName("config_peersets")
            .withValue(getPeersets())
            .endEnv()
            .addNewEnv()
            .withName("config_metricTest")
            .withValue("false")
            .endEnv()
            .addNewPort()
            .withContainerPort(80)
            .endPort()
            .editOrNewReadinessProbe()
            .editOrNewHttpGet()
            .withNewPort(80)
            .withPath("/healthcheck")
            .endHttpGet()
            .endReadinessProbe()
            .withImagePullPolicy("Always")
            .editOrNewResources()
            .addToRequests("cpu", Quantity.fromString(resourceCpu))
            .addToRequests("memory", Quantity.fromString(resourceMemory))
            .endResources()
            .withVolumeMounts(buildVolumeMounts())
            .build()
    }

    private fun buildVolumes(): List<V1Volume> {
        return listOf(
            V1VolumeBuilder()
                .withName(pvcName)
                .editOrNewPersistentVolumeClaim()
                .withClaimName(pvcName)
                .endPersistentVolumeClaim()
                .build(),
        )
    }

    private fun buildVolumeMounts(): List<V1VolumeMount> {
        return listOf(
            V1VolumeMountBuilder()
                .withName(pvcName)
                .withMountPath("/var/lib/redis")
                .build(),
        )
    }

    private fun buildService(old: V1Service): V1Service {
        return V1ServiceBuilder(old)
            .withApiVersion("v1")
            .withKind("Service")
            .editOrNewMetadata()
            .withName(serviceName)
            .endMetadata()
            .editOrNewSpec()
            .addToSelector(labels)
            .withPorts()
            .addNewPort()
            .withPort(80)
            .withNewTargetPort(80)
            .endPort()
            .endSpec()
            .withStatus(null)
            .build()
    }

    private fun buildPvc(old: V1PersistentVolumeClaim): V1PersistentVolumeClaim {
        val requests: MutableMap<String, Quantity> = HashMap()
        requests["storage"] = Quantity.fromString("3Gi")
        return V1PersistentVolumeClaimBuilder(old)
            .editOrNewMetadata()
            .withName(pvcName)
            .endMetadata()
            .editOrNewSpec()
            .withNewStorageClassName("local-path")
            .withAccessModes("ReadWriteOnce")
            .editOrNewResources()
            .withRequests(requests)
            .endResources()
            .endSpec()
            .build()
    }

    @Throws(ApiException::class)
    fun apply() {
        logger.info("Applying peer {}", peerId)
        applyPvc()
        applyDeployment()
        applyService()
    }

    @Throws(ApiException::class)
    private fun applyDeployment() {
        try {
            val oldDeployment = appsApi.readNamespacedDeploymentStatus(deploymentName, namespace, null)
            replaceDeployment(oldDeployment)
        } catch (e: ApiException) {
            if (e.code != 404) {
                throw e
            }

            createDeployment()
        }
    }

    @Throws(ApiException::class)
    private fun applyService() {
        try {
            val oldService = coreApi.readNamespacedService(serviceName, namespace, null, false, false)
            replaceService(oldService)
        } catch (e: ApiException) {
            if (e.code != 404) {
                throw e
            }

            createService()
        }
    }

    @Throws(ApiException::class)
    private fun applyPvc() {
        try {
            val oldPvc = coreApi.readNamespacedPersistentVolumeClaim(pvcName, namespace, null, false, false)
            replacePvc(oldPvc)
        } catch (e: ApiException) {
            if (e.code != 404) {
                throw e
            }

            createPvc()
        }
    }

    @Throws(ApiException::class)
    fun createDeployment() {
        logger.info("  Creating deployment...")
        val deployment = buildDeployment(V1Deployment())
        appsApi.createNamespacedDeployment(namespace, deployment, null, null, null)
    }

    @Throws(ApiException::class)
    fun createService() {
        logger.info("  Creating service...")
        val service = buildService(V1Service())
        coreApi.createNamespacedService(namespace, service, null, null, null)
    }

    @Throws(ApiException::class)
    fun createPvc() {
        logger.info("  Creating PVC...")
        val pvc = buildPvc(V1PersistentVolumeClaim())
        coreApi.createNamespacedPersistentVolumeClaim(namespace, pvc, null, null, null)
    }

    @Throws(ApiException::class)
    fun replaceDeployment(oldDeployment: V1Deployment) {
        logger.info("  Replacing deployment...")
        val deployment = buildDeployment(oldDeployment)
        appsApi.replaceNamespacedDeployment(deploymentName, namespace, deployment, null, null, null)
    }

    @Throws(ApiException::class)
    fun replaceService(oldService: V1Service) {
        logger.info("  Replacing service...")
        val service = buildService(oldService)
        coreApi.replaceNamespacedService(serviceName, namespace, service, null, null, null)
    }

    @Throws(ApiException::class)
    fun replacePvc(oldPvc: V1PersistentVolumeClaim) {
        logger.info("  Replacing PVC...")
        val pvc = buildPvc(oldPvc)
        coreApi.replaceNamespacedPersistentVolumeClaim(pvcName, namespace, pvc, null, null, null)
    }

    companion object {
        const val DEFAULT_IMAGE: String = "ghcr.io/kjarosh/depl-da-prototype:latest"

        private val logger = LoggerFactory.getLogger("k8s-peer")

        private val appsApi = AppsV1Api()
        private val coreApi = CoreV1Api()
    }
}
