package com.github.davenury.common

import com.sksamuel.hoplite.ConfigLoaderBuilder
import com.sksamuel.hoplite.addResourceSource
import com.sksamuel.hoplite.decoder.Decoder
import com.sksamuel.hoplite.sources.MapPropertySource

fun parsePeersets(peersets: String): Map<PeersetId, List<PeerId>> {
    return peersets.split(";").associate { parsePeerset(it) }
}

fun parsePeerset(peerset: String): Pair<PeersetId, List<PeerId>> {
    val split = peerset.split("=")
    return PeersetId(split[0].trim()) to split[1].split(",").map { PeerId(it.trim()) }
}

fun parsePeers(peers: String): Map<PeerId, PeerAddress> {
    return peers.split(";").associate { parsePeer(it) }
}

fun parsePeer(peer: String): Pair<PeerId, PeerAddress> {
    val split = peer.split("=")
    val peerId = PeerId(split[0].trim())
    return peerId to PeerAddress(peerId, split[1].trim())
}

inline fun <reified T> loadConfig(
    overrides: Map<String, Any> = emptyMap(),
    decoders: List<Decoder<*>> = listOf(),
): T {
    val configFile =
        System.getProperty("configFile")
            ?: System.getenv("CONFIG_FILE")
            ?: "application.conf"
    return loadConfig(configFile, overrides, decoders = decoders)
}

inline fun <reified T> loadConfig(
    configFileName: String,
    overrides: Map<String, Any> = emptyMap(),
    decoders: List<Decoder<*>> = listOf(),
): T =
    ConfigLoaderBuilder
        .default()
        .addSource(MapPropertySource(overrides))
        .addResourceSource("/$configFileName")
        .addDecoders(decoders)
        .build()
        .loadConfigOrThrow()
