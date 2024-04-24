package com.github.davenury.ucac.testcontainers

import com.github.davenury.common.AddUserChange
import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.Changes
import com.github.davenury.common.PeersetId
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.common.objectMapper
import com.github.davenury.ucac.utils.ApplicationTestcontainersEnvironment
import com.github.davenury.ucac.utils.TestLogExtension
import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.features.HttpTimeout
import io.ktor.client.features.json.JacksonSerializer
import io.ktor.client.features.json.JsonFeature
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.statement.HttpResponse
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
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
            install(JsonFeature) {
                serializer = JacksonSerializer(objectMapper)
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

    @Disabled("Failing locally")
    @Test
    fun `sync api`(): Unit =
        runBlocking {
            val change =
                AddUserChange(
                    "test user",
                    peersets =
                        listOf(
                            ChangePeersetInfo(PeersetId("peerset0"), InitialHistoryEntry.getId()),
                        ),
                )

            logger.info("Sending change $change")

            val peer0Address = environment.getAddress("peer0")
            val response =
                http.post<HttpResponse>("http://$peer0Address/v2/change/sync") {
                    contentType(ContentType.Application.Json)
                    body = change
                }
            expectThat(response.status.value).isEqualTo(201)

            val changes =
                http.get<Changes>("http://$peer0Address/v2/change") {
                    contentType(ContentType.Application.Json)
                    body = change
                }
            expectThat(changes).hasSize(1)
        }
}
