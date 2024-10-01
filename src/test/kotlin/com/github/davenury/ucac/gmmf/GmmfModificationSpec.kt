package com.github.davenury.ucac.gmmf

import com.github.davenury.ucac.gmmf.routing.EdgeMessage
import com.github.davenury.ucac.testHttpClient
import com.github.davenury.ucac.utils.IntegrationTestBase
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.Vertex
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.graph.model.ZoneId
import io.ktor.client.request.accept
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.statement.HttpResponse
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isSuccess

@ExtendWith(TestLogExtension::class)
class GmmfModificationSpec : IntegrationTestBase() {
    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "application-integration.conf")
    }

    @Test
    fun `add vertex`(): Unit =
        runBlocking {
            apps = TestApplicationSet(mapOf("peerset0" to listOf("peer0", "peer1")))

            expectCatching {
                addVertex("peer0", "peerset0", "user1", Vertex.Type.USER)
            }.isSuccess()

            expectCatching {
                addVertex("peer1", "peerset0", "user2", Vertex.Type.USER)
            }.isSuccess()

            val user1Peer0 = getVertex("peer0", "peerset0", "user1")
            val user2Peer0 = getVertex("peer0", "peerset0", "user2")
            val user1Peer1 = getVertex("peer1", "peerset0", "user1")
            val user2Peer1 = getVertex("peer1", "peerset0", "user2")

            expectThat(user1Peer0.name).isEqualTo("user1")
            expectThat(user1Peer0.type).isEqualTo(Vertex.Type.USER)
            expectThat(user2Peer0.name).isEqualTo("user2")
            expectThat(user2Peer0.type).isEqualTo(Vertex.Type.USER)
            expectThat(user1Peer1.name).isEqualTo("user1")
            expectThat(user1Peer1.type).isEqualTo(Vertex.Type.USER)
            expectThat(user2Peer1.name).isEqualTo("user2")
            expectThat(user2Peer1.type).isEqualTo(Vertex.Type.USER)
        }

    @Test
    fun `add edge`(): Unit =
        runBlocking {
            apps = TestApplicationSet(mapOf("peerset0" to listOf("peer0", "peer1")))

            expectCatching {
                addVertex("peer0", "peerset0", "user1", Vertex.Type.USER)
            }.isSuccess()

            expectCatching {
                addVertex("peer1", "peerset0", "user2", Vertex.Type.USER)
            }.isSuccess()

            expectCatching {
                addEdge(
                    "http://${apps.getPeer("peer0").address}/gmmf/graph/edge?peerset=peerset0",
                    VertexId(ZoneId("peerset0"), "user1"),
                    VertexId(ZoneId("peerset0"), "user2"),
                    Permissions("01010"),
                )
            }.isSuccess()

            val edgePeer0 = getEdge("http://${apps.getPeer("peer0").address}/gmmf/graph/edge/user1/user2?peerset=peerset0")
            val edgePeer1 = getEdge("http://${apps.getPeer("peer1").address}/gmmf/graph/edge/user1/user2?peerset=peerset0")

            expectThat(edgePeer0.permissions).isEqualTo(Permissions("01010"))
            expectThat(edgePeer1.permissions).isEqualTo(Permissions("01010"))
        }

    @Test
    fun `add external edge`(): Unit =
        runBlocking {
            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1"),
                        "peerset1" to listOf("peer2", "peer3"),
                    ),
                )

            expectCatching {
                addVertex("peer0", "peerset0", "user1", Vertex.Type.USER)
            }.isSuccess()

            expectCatching {
                addVertex("peer2", "peerset1", "user2", Vertex.Type.USER)
            }.isSuccess()

            expectCatching {
                addEdge(
                    "http://${apps.getPeer("peer0").address}/gmmf/graph/edge?peerset=peerset0",
                    VertexId(ZoneId("peerset0"), "user1"),
                    VertexId(ZoneId("peerset1"), "user2"),
                    Permissions("01010"),
                )
            }.isSuccess()
        }

    private suspend fun addEdge(
        url: String,
        from: VertexId,
        to: VertexId,
        permissions: Permissions,
    ) {
        testHttpClient.post<HttpResponse>(url) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = EdgeMessage(from, to, permissions)
        }
    }

    private suspend fun getEdge(url: String): EdgeMessage {
        return testHttpClient.get<EdgeMessage>(url) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(GmmfModificationSpec::class.java)
    }
}
