package com.github.davenury.ucac.consensus.paxos

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.consensus.ConsensusProtocolClient
import com.github.davenury.ucac.consensus.ConsensusProtocolClientImpl
import com.github.davenury.ucac.consensus.ConsensusResponse
import com.github.davenury.ucac.httpClient
import io.ktor.client.call.body
import io.ktor.client.request.accept
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.contentType

interface PigPaxosProtocolClient : ConsensusProtocolClient {
    suspend fun sendProposes(
        peers: List<PeerAddress>,
        message: PaxosPropose,
    ): List<ConsensusResponse<PaxosPromise?>>

    suspend fun sendAccept(
        peer: PeerAddress,
        message: PaxosAccept,
    ): ConsensusResponse<PaxosAccepted?>

    suspend fun sendCommit(
        peer: PeerAddress,
        message: PaxosCommit,
    ): ConsensusResponse<PaxosCommitResponse?>

    suspend fun sendBatchCommit(
        peer: PeerAddress,
        message: PaxosBatchCommit,
    ): ConsensusResponse<String?>

    suspend fun sendRequestApplyChange(
        peer: PeerAddress,
        change: Change,
    ): ChangeResult
}

class PigPaxosProtocolClientImpl(override val peersetId: PeersetId) : PigPaxosProtocolClient, ConsensusProtocolClientImpl(
    peersetId,
) {
    override suspend fun sendProposes(
        peers: List<PeerAddress>,
        message: PaxosPropose,
    ): List<ConsensusResponse<PaxosPromise?>> {
        logger.debug("Sending proposes requestes to ${peers.map { it.peerId }}")
        return peers
            .map { Pair(it, message) }
            .let { sendRequests(it, "protocols/paxos/propose") }
    }

    override suspend fun sendAccept(
        peer: PeerAddress,
        message: PaxosAccept,
    ): ConsensusResponse<PaxosAccepted?> {
        logger.debug("Sending accept request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "protocols/paxos/accept")
    }

    override suspend fun sendCommit(
        peer: PeerAddress,
        message: PaxosCommit,
    ): ConsensusResponse<PaxosCommitResponse?> {
        logger.debug("Sending commit request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "protocols/paxos/commit")
    }

    override suspend fun sendBatchCommit(
        peer: PeerAddress,
        message: PaxosBatchCommit,
    ): ConsensusResponse<String?> {
        logger.debug("Sending batch commit request to ${peer.peerId}")
        return sendRequest(Pair(peer, message), "protocols/paxos/batch-commit")
    }

    override suspend fun sendRequestApplyChange(
        peer: PeerAddress,
        change: Change,
    ): ChangeResult =
        httpClient.post("http://${peer.address}/protocols/paxos/request_apply_change?peerset=$peersetId") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            setBody(change)
        }.body()
}
