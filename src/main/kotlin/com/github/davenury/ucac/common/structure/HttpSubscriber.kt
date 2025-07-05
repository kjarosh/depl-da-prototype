package com.github.davenury.ucac.common.structure

import com.github.davenury.common.CurrentLeaderFullInfoDto
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.httpClient
import io.ktor.client.call.body
import io.ktor.client.request.accept
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpStatement
import io.ktor.http.ContentType
import io.ktor.http.contentType
import org.slf4j.LoggerFactory

class HttpSubscriber(
    private val url: String,
) : Subscriber {
    override suspend fun notifyConsensusLeaderChange(
        newLeaderPeerId: PeerId,
        newLeaderPeersetId: PeersetId,
    ) {
        logger.info("Sending new consensus leader message to $url")
        try {
            val response =
                httpClient.post(url) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    setBody(CurrentLeaderFullInfoDto(newLeaderPeerId, newLeaderPeersetId))
                }.body<HttpStatement>()
            logger.info("Sent new consensus leader change to notification service: ${response.execute().status.value}")
        } catch (e: Exception) {
            logger.error("Could not send notification to subscriber", e)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("HttpSubscriber")
    }
}
