package com.github.davenury.ucac.gmmf.tests.k8s

import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1ConfigMap
import io.kubernetes.client.openapi.models.V1ConfigMapBuilder

/**
 * @author Kamil Jarosz
 */
class K8sGraphConfigMap(private val namespace: String, private val name: String, private val data: ByteArray) {
    private fun buildConfigMap(base: V1ConfigMap): V1ConfigMap {
        return V1ConfigMapBuilder(base)
            .editOrNewMetadata()
            .withName(name)
            .endMetadata()
            .removeFromBinaryData("graph.json")
            .addToBinaryData("graph.json", data)
            .build()
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
