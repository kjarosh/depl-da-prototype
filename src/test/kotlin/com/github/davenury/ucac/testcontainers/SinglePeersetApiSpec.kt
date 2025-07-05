package com.github.davenury.ucac.testcontainers

import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.Changes
import com.github.davenury.common.PeersetId
import com.github.davenury.common.StandardChange
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.utils.ApplicationTestcontainersEnvironment
import com.github.davenury.ucac.utils.TestLogExtension
import io.ktor.client.HttpClient
import io.ktor.client.call.body
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.ktor.serialization.jackson.jackson
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import strikt.api.expectThat
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo

/**
 * @author Kamil Jarosz
 */
@Testcontainers
@ExtendWith(TestLogExtension::class)
class SinglePeersetApiSpec {
    companion object {
        private val logger = LoggerFactory.getLogger(SinglePeersetApiSpec::class.java)
    }

    private val http =
        HttpClient(OkHttp) {
            expectSuccess = true
            install(ContentNegotiation) {
                jackson()
            }
            install(HttpTimeout) {
                socketTimeoutMillis = 120000
            }
        }

    @Container
    private val environment =
        ApplicationTestcontainersEnvironment(
            mapOf(
                "peerset0" to listOf("peer0", "peer1", "peer2"),
            ),
        )

    @Test
    fun `sync api`(): Unit =
        runBlocking {
            val change =
                StandardChange(
                    "test change",
                    peersets =
                        listOf(
                            ChangePeersetInfo(PeersetId("peerset0"), InitialHistoryEntry.getId()),
                        ),
                )

            logger.info("Sending change $change")

            val peer0Address = environment.getAddress("peer0")
            val response =
                http.post("http://$peer0Address/v2/change/sync?peerset=peerset0") {
                    contentType(ContentType.Application.Json)
                    setBody(change)
                }.body<HttpResponse>()
            expectThat(response.status.value).isEqualTo(201)

            val changes =
                http.get("http://$peer0Address/v2/change?peerset=peerset0") {
                    contentType(ContentType.Application.Json)
                    setBody(change)
                }.body<Changes>()
            expectThat(changes).hasSize(1)
        }
}
