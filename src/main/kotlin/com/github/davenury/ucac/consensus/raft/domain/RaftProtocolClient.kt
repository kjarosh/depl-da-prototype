package com.github.davenury.ucac.consensus.raft.domain

import com.github.davenury.ucac.raftHttpClient
import io.ktor.client.request.*
import io.ktor.http.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import org.slf4j.LoggerFactory


interface RaftProtocolClient {

    suspend fun sendConsensusElectMe(
        otherPeers: List<String>,
        message: ConsensusElectMe
    ): List<RaftResponse<ConsensusElectedYou?>>

    suspend fun sendConsensusImTheLeader(
        otherPeers: List<String>,
        message: ConsensusImTheLeader
    ): List<RaftResponse<String?>>

    suspend fun sendConsensusHeartbeat(peersWithMessage: List<Pair<String, ConsensusHeartbeat>>): List<RaftResponse<ConsensusHeartbeatResponse?>>
    suspend fun sendConsensusHeartbeat(peerWithMessage: Pair<String, ConsensusHeartbeat>): RaftResponse<ConsensusHeartbeatResponse?>
}

class RaftProtocolClientImpl(private val id: Int) : RaftProtocolClient {

    override suspend fun sendConsensusElectMe(
        otherPeers: List<String>,
        message: ConsensusElectMe
    ): List<RaftResponse<ConsensusElectedYou?>> =
        otherPeers
            .map { Pair(it, message) }
            .let { sendRequests(it, "consensus/request_vote") }

    override suspend fun sendConsensusImTheLeader(
        otherPeers: List<String>,
        message: ConsensusImTheLeader
    ): List<RaftResponse<String?>> =
        otherPeers
            .map { Pair(it, message) }
            .let { sendRequests(it, "consensus/leader") }

    override suspend fun sendConsensusHeartbeat(
        peersWithMessage: List<Pair<String, ConsensusHeartbeat>>
    ): List<RaftResponse<ConsensusHeartbeatResponse?>> =
        sendRequests(peersWithMessage, "consensus/heartbeat")

    override suspend fun sendConsensusHeartbeat(
        peerWithMessage: Pair<String, ConsensusHeartbeat>
    ): RaftResponse<ConsensusHeartbeatResponse?> =
        CoroutineScope(Dispatchers.IO).async {
            sendConsensusMessage<ConsensusHeartbeat, ConsensusHeartbeatResponse>(
                "http://${peerWithMessage.first}/consensus/heartbeat",
                peerWithMessage.second
            )
        }.let {
            Pair(peerWithMessage.first, it)
        }.let {
            val result = try {
                it.second.await()
            } catch (e: Exception) {
                logger.error("$id - Error while evaluating response from ${it.first}: $e", e)
                null
            }
            RaftResponse(it.first, result)
        }

    private suspend inline fun <T, reified K> sendRequests(
        peersWithBody: List<Pair<String, T>>,
        urlPath: String
    ): List<RaftResponse<K?>> =
        peersWithBody.map {
            CoroutineScope(Dispatchers.IO).async {
                sendConsensusMessage<T, K>("http://${it.first}/$urlPath", it.second)
            }.let { coroutine ->
                Pair(it.first, coroutine)
            }
        }.map {
            val result = try {
                it.second.await()
            } catch (e: Exception) {
                logger.error("$id - Error while evaluating response from ${it.first}: $e", e)
                null
            }

            RaftResponse(it.first, result)
        }


    private suspend inline fun <Message, reified Response> sendConsensusMessage(
        url: String,
        message: Message
    ): Response? {
        logger.info("$id - Sending to: $url")
        return raftHttpClient.post<Response>(url) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = message!!
        }
    }


    companion object {
        private val logger = LoggerFactory.getLogger(RaftProtocolClientImpl::class.java)
    }
}

data class RaftResponse<K>(val from: String, val message: K)
