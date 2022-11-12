package com.github.davenury.ucac.utils

import com.github.davenury.ucac.ApplicationUcac
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.consensus.ConsensusSpec
import com.github.davenury.ucac.createApplication
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.random.Random

class TestApplicationSet(
    numberOfPeersInPeersets: List<Int>,
    signalListeners: Map<Int, Map<Signal, SignalListener>> = emptyMap(),
    configOverrides: Map<Int, Map<String, Any>> = emptyMap(),
    val appsToExclude: List<Int> = emptyList()
) {

    private var apps: MutableList<MutableList<ApplicationUcac>> = mutableListOf()
    private val peers: List<List<String>>

    init {
        val numberOfPeersets = numberOfPeersInPeersets.size
        val testConfigOverrides = mapOf(
            "ratis.addresses" to List(numberOfPeersets) {
                List(numberOfPeersInPeersets[it]) { "localhost:${Random.nextInt(5000, 20000) + 11124}" }
            }.joinToString(";") { it.joinToString(",") },
            "peers" to (0 until numberOfPeersets).map { (0 until numberOfPeersInPeersets[it]).map { NON_RUNNING_PEER } }
                .joinToString(";") { it.joinToString(",") },
            "port" to 0,
            "host" to "localhost",
        )

        var currentApp = 0
        apps = (0 until numberOfPeersets).map { peersetId ->
            (0 until numberOfPeersInPeersets[peersetId]).map { peerId ->
                createApplication(
                    signalListeners[currentApp++] ?: emptyMap(),
                    mapOf("peerId" to peerId, "peersetId" to peersetId) +
                            testConfigOverrides +
                            (configOverrides[peerId] ?: emptyMap()),
                )
            }.toMutableList()
        }.toMutableList()

        validateAppIds(signalListeners.keys, currentApp)
        validateAppIds(configOverrides.keys, currentApp)
        validateAppIds(appsToExclude, currentApp)

        // start and address discovery
        apps
            .flatten()
            .filterIndexed { index, _ -> !appsToExclude.contains(index) }
            .forEach { it.startNonblocking() }
        peers =
            apps.flatten()
                .asSequence()
                .mapIndexed { index, it ->
                    val address = if (index in appsToExclude) NON_RUNNING_PEER else "localhost:${it.getBoundPort()}"
                    Pair(it, address)
                }
                .groupBy { it.first.getPeersetId() }
                .values
                .map { it.map { it.second } }
                .toList()

        apps
            .flatten()
            .filterIndexed { index, _ -> !appsToExclude.contains(index) }
            .forEach { app ->
                app.setPeers(peers)
            }

        logger.info("Apps ready")
    }

    private fun validateAppIds(
        appIds: Collection<Int>,
        appCount: Int,
    ) {
        if (appIds.isNotEmpty()) {
            val sorted = TreeSet(appIds)
            if (sorted.first() < 0 || sorted.last() >= appCount) {
                throw AssertionError("Wrong app IDs: $sorted (total number of apps: $appCount)")
            }
        }
    }

    fun stopApps(gracePeriodMillis: Long = 200, timeoutPeriodMillis: Long = 1000) {
        logger.info("Stopping apps")
        apps.flatten().forEach { it.stop(gracePeriodMillis, timeoutPeriodMillis) }
    }

    fun getPeers() = peers

    fun getRunningPeers() = peers.map { it.filter { it != NON_RUNNING_PEER } }

    fun getRunningApps(): List<ApplicationUcac> = apps
        .flatten()
        .zip(peers.flatten())
        .filterIndexed { index, _ -> !appsToExclude.contains(index) }
        .map { it.first }

    companion object {
        const val NON_RUNNING_PEER: String = "localhost:0"
        private val logger = LoggerFactory.getLogger(TestApplicationSet::class.java)
    }
}
