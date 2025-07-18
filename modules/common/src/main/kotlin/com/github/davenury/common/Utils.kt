package com.github.davenury.common

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.server.application.ApplicationCall
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import org.slf4j.LoggerFactory
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant

val objectMapper: ObjectMapper =
    jacksonObjectMapper().configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true)

fun sha512(string: String): String {
    val md = MessageDigest.getInstance("SHA-512")
    val digest = md.digest(string.toByteArray(Charsets.UTF_8))
    return digest.joinToString(separator = "") { eachByte -> "%02x".format(eachByte) }
}

val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)

object Metrics {
    private val changeIdToTimer: MutableMap<String, Instant> = mutableMapOf()

    private var lastHeartbeat: Instant = Instant.now()

    fun bumpChangeProcessed(
        changeResult: ChangeResult,
        protocol: String,
        peersetId: PeersetId,
    ) {
        Counter
            .builder("change_processed")
            .tag("result", changeResult.status.name.lowercase())
            .tag("protocol", protocol)
            .tag("peerset_id", peersetId.peersetId)
            .register(meterRegistry)
            .increment()
    }

    fun bumpLeaderElection(
        peerId: PeerId,
        peersetId: PeersetId,
    ) {
        Counter
            .builder("leader_elected")
            .tag("peerId", peerId.peerId.lowercase())
            .tag("peersetId", peersetId.peersetId.lowercase())
            .tag("protocol", "consensus")
            .register(meterRegistry)
            .increment()
    }

    fun startTimer(changeId: String) {
        changeIdToTimer[changeId] = Instant.now()
    }

    fun stopTimer(
        changeId: String,
        protocol: String,
        result: ChangeResult,
    ) {
        val timeElapsed = Duration.between(changeIdToTimer[changeId]!!, Instant.now())
        logger.info("Time elapsed for change: $changeId: $timeElapsed")
        Timer
            .builder("change_processing_time")
            .tag("protocol", protocol)
            .tag("result", result.status.name.lowercase())
            .register(meterRegistry)
            .record(timeElapsed)
    }

    fun synchronizationTimer(
        peerId: PeerId,
        timeElapsed: Duration,
    ) {
        logger.info("Time elapsed for changes synchronization on peer: $peerId: $timeElapsed")
        Timer
            .builder("changes_synchronization_time")
            .tag("protocol", "consensus")
            .register(meterRegistry)
            .record(timeElapsed)
    }

    fun refreshLastHeartbeat() {
        lastHeartbeat = Instant.now()
    }

    fun registerTimerHeartbeat() {
        val now = Instant.now()
        Timer
            .builder("heartbeat_processing_time")
            .register(meterRegistry)
            .record(Duration.between(lastHeartbeat, now))
        lastHeartbeat = now
    }

    fun bumpChangeMetric(
        changeId: String,
        peerId: PeerId,
        peersetId: PeersetId,
        protocolName: ProtocolName,
        state: String,
    ) {
        Counter.builder("change_state_changed")
            .tag("change_id", changeId)
            .tag("peer_id", peerId.toString())
            .tag("peerset_id", peersetId.toString())
            .tag("protocol", protocolName.name.lowercase())
            .tag("state", state)
            .register(meterRegistry)
            .increment()
    }

    private val logger = LoggerFactory.getLogger("Metrics")
}

fun ApplicationCall.peersetId() =
    PeersetId(
        this.request.queryParameters["peerset"] ?: throw MissingPeersetParameterException(),
    )
