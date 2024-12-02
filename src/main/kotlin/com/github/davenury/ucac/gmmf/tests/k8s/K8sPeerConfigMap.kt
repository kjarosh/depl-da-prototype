package com.github.davenury.ucac.gmmf.tests.k8s

import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1ConfigMap
import io.kubernetes.client.openapi.models.V1ConfigMapBuilder

/**
 * @author Kamil Jarosz
 */
class K8sPeerConfigMap(
    private val name: String,
    private val namespace: String,
    private val peersets: List<Int>,
) {
    private fun buildConfigMap(base: V1ConfigMap): V1ConfigMap {
        return V1ConfigMapBuilder(base)
            .editOrNewMetadata()
            .withName(name)
            .endMetadata()
            .withData(
                mapOf(
                    Pair("CONFIG_FILE", "application-kubernetes.conf"),
                    Pair("config_peers", getPeers()),
                    Pair("config_peersets", getPeersets()),
                ),
            )
            .build()
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

    @Throws(ApiException::class)
    fun apply() {
        try {
            val oldConfigMap = coreApi.readNamespacedConfigMap(name, namespace, null, null, null)
            replace(oldConfigMap)
        } catch (e: ApiException) {
            if (e.code != 404) {
                throw e
            }

            create()
        }
    }

    @Throws(ApiException::class)
    private fun create() {
        coreApi.createNamespacedConfigMap(
            namespace,
            buildConfigMap(V1ConfigMap()),
            null,
            null,
            null,
        )
    }

    @Throws(ApiException::class)
    private fun replace(old: V1ConfigMap) {
        val configMap = buildConfigMap(old)
        coreApi.replaceNamespacedConfigMap(name, namespace, configMap, null, null, null)
    }

    companion object {
        private val coreApi = CoreV1Api()
    }
}
