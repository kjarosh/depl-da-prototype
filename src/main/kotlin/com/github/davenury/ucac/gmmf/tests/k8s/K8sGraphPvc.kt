package com.github.davenury.ucac.gmmf.tests.k8s

import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimBuilder

/**
 * @author Kamil Jarosz
 */
class K8sGraphPvc(private val namespace: String, private val name: String) {
    private fun buildPvc(base: V1PersistentVolumeClaim): V1PersistentVolumeClaim {
        return V1PersistentVolumeClaimBuilder(base)
            .editOrNewMetadata()
            .withName(name)
            .endMetadata()
            .editOrNewSpec()
            .withNewStorageClassName("nfs")
            .withAccessModes("ReadWriteMany")
            .editOrNewResources()
            .addToRequests("storage", Quantity.fromString("1Gi"))
            .endResources()
            .endSpec()
            .build()
    }

    @Throws(ApiException::class)
    fun apply() {
        try {
            val oldPvc = coreApi.readNamespacedPersistentVolumeClaim(name, namespace, null, null, null)
            replace(oldPvc)
        } catch (e: ApiException) {
            if (e.code != 404) {
                throw e
            }

            create()
        }
    }

    @Throws(ApiException::class)
    private fun create() {
        coreApi.createNamespacedPersistentVolumeClaim(
            namespace,
            buildPvc(V1PersistentVolumeClaim()),
            null,
            null,
            null,
        )
    }

    @Throws(ApiException::class)
    private fun replace(old: V1PersistentVolumeClaim) {
        val pvc = buildPvc(old)
        coreApi.replaceNamespacedPersistentVolumeClaim(name, namespace, pvc, null, null, null)
    }

    companion object {
        private val coreApi = CoreV1Api()
    }
}
