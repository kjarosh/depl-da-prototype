package com.github.davenury.ucac.utils

import com.github.davenury.ucac.*
import com.github.davenury.ucac.SignalListener
import java.util.*
import kotlin.random.Random

class TestApplicationSet(
    numberOfPeersets: Int,
    numberOfPeersInPeersets: List<Int>,
    signalListeners: Map<Int, Map<Signal, SignalListener>> = emptyMap(),
    configOverrides: Map<Int, Map<String, Any>> = emptyMap(),
    appsToExclude: List<Int> = emptyList()
) {

    private var apps: MutableList<MutableList<Application>> = mutableListOf()
    private val peers: List<List<String>>

    init {
        val ratisConfigOverrides = mapOf(
            "raft.server.addresses" to List(numberOfPeersets) {
                List(numberOfPeersInPeersets[it]) { "localhost:${Random.nextInt(5000, 20000) + 11124}" }
            },
            "raft.clusterGroupIds" to List(numberOfPeersets) { UUID.randomUUID() }
        )

        var currentApp = 0
        apps = MutableList(numberOfPeersets) { peersetId ->
            MutableList(numberOfPeersInPeersets[peersetId]) { peerId ->
                currentApp++
                createApplication(
                    arrayOf("${peerId + 1}", "${peersetId + 1}"),
                    signalListeners[currentApp + 1] ?: emptyMap(),
                    ratisConfigOverrides + (configOverrides[peerId] ?: emptyMap()),
                    TestApplicationMode(peerId + 1, peersetId + 1)
                )
            }
        }

        // start and address discovery
        apps.flatten().forEachIndexed { index, app -> if (index + 1 !in appsToExclude) app.startNonblocking() }
        peers =
            apps.flatten()
                .asSequence()
                .mapIndexed {index, it -> Pair(it, if (index + 1 in appsToExclude) "localhost:0" else "localhost:${it.getBoundPort()}") }
                .groupBy{ it.first.getPeersetId() }
                .values
                .map { it.map { it.second } }
                .toList()

        apps.flatten().zip(peers.flatten()).forEachIndexed { index, (app, peer) ->
            if (index + 1 !in appsToExclude) {
                app.setOtherPeers(
                    peers.map { it.filterNot { it == peer } }
                )
            }
        }
    }

    fun stopApps(gracePeriodMillis: Long = 200, timeoutPeriodMillis: Long = 1000) {
        apps.flatten().forEach { it.stop(gracePeriodMillis, timeoutPeriodMillis) }
    }

    fun getPeers() = peers

}
