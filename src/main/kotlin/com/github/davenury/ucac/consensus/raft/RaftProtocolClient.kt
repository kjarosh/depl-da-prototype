package com.github.davenury.ucac.consensus.raft

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.consensus.ConsensusProtocolClient
import com.github.davenury.ucac.consensus.ConsensusProtocolClientImpl
import com.github.davenury.ucac.consensus.ConsensusResponse
import com.github.davenury.ucac.httpClient
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.plugins.ClientRequestException
import io.ktor.client.request.accept
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory

interface RaftProtocolClient : ConsensusProtocolClient {
    suspend fun sendConsensusElectMe(
        otherPeers: List<PeerAddress>,
        message: ConsensusElectMe,
    ): List<ConsensusResponse<ConsensusElectedYou?>>

    suspend fun sendConsensusHeartbeat(
        peer: PeerAddress,
        message: ConsensusHeartbeat,
        httpClient: HttpClient,
    ): Deferred<ConsensusResponse<ConsensusHeartbeatResponse?>>

    suspend fun sendRequestApplyChange(
        address: String,
        change: Change,
    ): ChangeResult
}

class RaftProtocolClientImpl(override val peersetId: PeersetId) : RaftProtocolClient,
    ConsensusProtocolClientImpl(peersetId) {
    override suspend fun sendConsensusElectMe(
        otherPeers: List<PeerAddress>,
        message: ConsensusElectMe,
    ): List<ConsensusResponse<ConsensusElectedYou?>> {
        logger.debug("Sending elect me requests to ${otherPeers.map { it.peerId }}")
        return otherPeers
            .map { Pair(it, message) }
            .let { sendRequests(it, "protocols/raft/request_vote") }
    }

    override suspend fun sendConsensusHeartbeat(
        peer: PeerAddress,
        message: ConsensusHeartbeat,
        httpClient: HttpClient,
    ): Deferred<ConsensusResponse<ConsensusHeartbeatResponse?>> {
        logger.debug("Sending heartbeat to ${peer.peerId}")
        return CoroutineScope(Dispatchers.IO).async(MDCContext()) {
            try {
                sendConsensusMessage<ConsensusHeartbeat, ConsensusHeartbeatResponse>(
                    peer,
                    "protocols/raft/heartbeat",
                    message,
                ).let { ConsensusResponse(peer.address, it) }
            } catch (e: CancellationException) {
                logger.debug("Canceled sendConsensusHeartbeat")
                ConsensusResponse(peer.address, null)
            } catch (e: Exception) {
                when {
                    e is ClientRequestException && e.response.status == HttpStatusCode.Unauthorized -> {
                        logger.error("Received unauthorized response from peer: ${peer.peerId}", e)
                        ConsensusResponse(peer.address, null, true)
                    }

                    else -> {
                        logger.error("Error while evaluating response from ${peer.peerId}", e)
                        ConsensusResponse(peer.address, null)
                    }
                }
            }
        }
    }

    override suspend fun sendRequestApplyChange(
        address: String,
        change: Change,
    ) = httpClient.post("http://$address/protocols/raft/request_apply_change?peerset=$peersetId") {
        contentType(ContentType.Application.Json)
        accept(ContentType.Application.Json)
        setBody(change)
    }.body<ChangeResult>()

    companion object {
        private val logger = LoggerFactory.getLogger("raft-client")
    }
}
