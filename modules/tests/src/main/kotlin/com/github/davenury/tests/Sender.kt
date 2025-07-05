package com.github.davenury.tests

import com.github.davenury.common.Change
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.common.PeersetInformationDto
import io.ktor.client.call.body
import io.ktor.client.plugins.ClientRequestException
import io.ktor.client.plugins.ServerResponseException
import io.ktor.client.request.accept
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.contentType
import org.slf4j.LoggerFactory
import java.io.IOException

interface Sender {
    suspend fun executeChange(
        address: PeerAddress,
        change: Change,
        peersetId: PeersetId,
    ): ChangeState

    suspend fun getConsensusLeaderId(
        address: PeerAddress,
        peersetId: PeersetId,
    ): PeerId?
}

class HttpSender(
    private val acProtocolConfig: ACProtocolConfig,
) : Sender {
    override suspend fun executeChange(
        address: PeerAddress,
        change: Change,
        peersetId: PeersetId,
    ): ChangeState {
        return try {
            logger.info("Sending $change to $address")
            Metrics.bumpSentChanges()
            val response =
                httpClient.post(
                    "http://${address.address}/v2/change/async?${acProtocolConfig.protocol.getParam(
                        acProtocolConfig.enforceUsage,
                    )}&peerset=${peersetId.peersetId}",
                ) {
                    accept(ContentType.Application.Json)
                    contentType(ContentType.Application.Json)
                    setBody(change)
                }
            logger.info("Received: ${response.status.value}")
            ChangeState.ACCEPTED
        } catch (e: Exception) {
            logger.error("Couldn't execute change with address: $address", e)
            when (e) {
                is ClientRequestException -> Metrics.reportUnsuccessfulChange(e.response.status.value)
                is ServerResponseException -> Metrics.reportUnsuccessfulChange(e.response.status.value)
                else -> throw e
            }
            ChangeState.REJECTED
        }
    }

    override suspend fun getConsensusLeaderId(
        address: PeerAddress,
        peersetId: PeersetId,
    ): PeerId? {
        return try {
            httpClient.get(
                "http://${address.address}/peerset-information?peerset=${peersetId.peersetId}",
            ).body<PeersetInformationDto>()
                .toDomain()
                .currentConsensusLeader
        } catch (e: IOException) {
            logger.error("Address: ${address.address} is dead, propagating exception", e)
            throw e
        } catch (e: Exception) {
            logger.error("Error while asking for consensus leader", e)
            null
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("TestsHttpSender")
    }
}
