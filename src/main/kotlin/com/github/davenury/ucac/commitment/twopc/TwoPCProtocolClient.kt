package com.github.davenury.ucac.commitment.twopc

import com.github.davenury.common.Change
import com.github.davenury.common.CurrentLeaderFullInfoDto
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.httpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.RedirectResponseException
import io.ktor.client.request.accept
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory

data class TwoPCRequestResponse(
    val success: Boolean,
    val redirect: Boolean = false,
    val newConsensusLeaderId: PeerId? = null,
    val newConsensusLeaderPeersetId: PeersetId? = null,
    val peersetId: PeersetId,
)

interface TwoPCProtocolClient {
    suspend fun sendAccept(
        peers: Map<PeersetId, PeerAddress>,
        change: Change,
    ): Map<PeerAddress, TwoPCRequestResponse>

    suspend fun sendDecision(
        peers: Map<PeersetId, PeerAddress>,
        decisionChange: Change,
    ): Map<PeerAddress, TwoPCRequestResponse>

    suspend fun askForChangeStatus(
        peer: PeerAddress,
        change: Change,
        otherPeerset: PeersetId,
    ): Change?
}

class TwoPCProtocolClientImpl : TwoPCProtocolClient {
    override suspend fun sendAccept(
        peers: Map<PeersetId, PeerAddress>,
        change: Change,
    ): Map<PeerAddress, TwoPCRequestResponse> = sendMessages(peers, change, "protocols/2pc/accept")

    override suspend fun sendDecision(
        peers: Map<PeersetId, PeerAddress>,
        decisionChange: Change,
    ): Map<PeerAddress, TwoPCRequestResponse> = sendMessages(peers, decisionChange, "protocols/2pc/decision")

    override suspend fun askForChangeStatus(
        peer: PeerAddress,
        change: Change,
        peersetId: PeersetId,
    ): Change? {
        val url = "http://${peer.address}/protocols/2pc/ask/${change.id}?peerset=$peersetId"
        logger.debug("Sending to: $url")
        return try {
            httpClient.get(url) {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }.body<Change?>()
        } catch (e: Exception) {
            logger.error("Error while evaluating response from $peer: $e", e)
            null
        }
    }

    private suspend fun <T> sendMessages(
        peers: Map<PeersetId, PeerAddress>,
        body: T,
        urlPath: String,
    ): Map<PeerAddress, TwoPCRequestResponse> =
        peers.map { (peersetId, peerAddress) ->
            CoroutineScope(Dispatchers.IO).async(MDCContext()) {
                send2PCMessage<T, Unit>("http://${peerAddress.address}/$urlPath?peerset=$peersetId", body)
            }.let { coroutine ->
                Triple(peerAddress, peersetId, coroutine)
            }
        }.associate {
            val result =
                try {
                    it.third.await()
                    it.first to
                        TwoPCRequestResponse(
                            success = true,
                            peersetId = it.second,
                        )
                } catch (e: RedirectResponseException) {
                    logger.info("Peer ${it.first} responded with redirect")
                    val newConsensusLeaderId = e.response.body<CurrentLeaderFullInfoDto>()
                    it.first to
                        TwoPCRequestResponse(
                            success = false,
                            redirect = true,
                            newConsensusLeaderId = newConsensusLeaderId.peerId,
                            newConsensusLeaderPeersetId = newConsensusLeaderId.peersetId,
                            peersetId = it.second,
                        )
                } catch (e: Exception) {
                    logger.error("Error while evaluating response from ${it.first}", e)
                    it.first to
                        TwoPCRequestResponse(
                            success = false,
                            peersetId = it.second,
                        )
                }
            result
        }

    private suspend inline fun <Message, reified Response> send2PCMessage(
        url: String,
        message: Message,
    ): Response? {
        logger.debug("Sending to: $url")
        return httpClient.post(url) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            setBody(message!!)
        }.body<Response>()
    }

    companion object {
        private val logger = LoggerFactory.getLogger("2pc-client")
    }
}
