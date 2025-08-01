package com.github.davenury.ucac.consensus

import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.raftHttpClient
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.ClientRequestException
import io.ktor.client.request.accept
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory

interface ConsensusProtocolClient {
    suspend fun sendLatestEntryIdQuery(
        peers: List<PeerAddress>,
        entryId: String,
    ): List<ConsensusResponse<LatestEntryIdResponse?>>
}

open class ConsensusProtocolClientImpl(open val peersetId: PeersetId) : ConsensusProtocolClient {
    suspend inline fun <reified T, reified K> sendRequest(
        peerWithBody: Pair<PeerAddress, T>,
        urlPath: String,
        httpClient: HttpClient = raftHttpClient,
    ): ConsensusResponse<K?> =
        CoroutineScope(Dispatchers.IO).async(MDCContext()) {
            sendConsensusMessage<T, K>(
                peerWithBody.first,
                urlPath,
                peerWithBody.second,
                httpClient,
            )
        }.let {
            try {
                val result = it.await()
                ConsensusResponse(peerWithBody.first.address, result)
            } catch (e: Exception) {
                when {
                    e is ClientRequestException && e.response.status == HttpStatusCode.Unauthorized -> {
                        logger.error("Received unauthorized response from peer: ${peerWithBody.first.peerId}")
                        ConsensusResponse(peerWithBody.first.address, null, true)
                    }

                    else -> {
                        logger.error("Error while evaluating response from ${peerWithBody.first.peerId}", e)
                        ConsensusResponse(peerWithBody.first.address, null)
                    }
                }
            }
        }

    suspend inline fun <reified Message, reified Response> sendConsensusMessage(
        peer: PeerAddress,
        suffix: String,
        message: Message,
        httpClient: HttpClient = raftHttpClient,
    ): Response? {
        logger.debug("Sending request to: ${peer.peerId} ${peer.address}, message: $message")
        return httpClient.post("http://${peer.address}/$suffix?peerset=$peersetId") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            setBody(message!!)
        }.body<Response>()
    }

    suspend inline fun <reified T, reified K> sendRequests(
        peersWithBody: List<Pair<PeerAddress, T>>,
        urlPath: String,
    ): List<ConsensusResponse<K?>> =
        peersWithBody.map {
            val peer = it.first
            val body = it.second
            CoroutineScope(Dispatchers.IO).async(MDCContext()) {
                sendConsensusMessage<T, K>(peer, urlPath, body)
            }.let { coroutine ->
                Pair(peer, coroutine)
            }
        }.map {
            val result =
                try {
                    it.second.await()
                } catch (e: Exception) {
                    if (it.first.address.endsWith(":0")) {
                        // Peer address is not valid, this may happen
                        // e.g. when initializing peers in tests.
                    } else {
                        logger.error("Error while evaluating response from ${it.first}", e)
                    }
                    null
                }

            ConsensusResponse(it.first.address, result)
        }

    override suspend fun sendLatestEntryIdQuery(
        peers: List<PeerAddress>,
        entryId: String,
    ): List<ConsensusResponse<LatestEntryIdResponse?>> =
        sendRequests(
            peers.map {
                Pair(it, entryId)
            },
            "/consensus/latest-entry",
        )

    companion object {
        val logger = LoggerFactory.getLogger("consensus-client")
    }
}

data class ConsensusResponse<K>(val from: String, val message: K, val unauthorized: Boolean = false)
