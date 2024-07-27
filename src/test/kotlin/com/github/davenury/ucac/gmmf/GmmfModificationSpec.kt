package com.github.davenury.ucac.gmmf

import com.github.davenury.ucac.gmmf.routing.NewEdge
import com.github.davenury.ucac.gmmf.routing.NewVertex
import com.github.davenury.ucac.testHttpClient
import com.github.davenury.ucac.utils.IntegrationTestBase
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.Vertex
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.graph.model.ZoneId
import io.ktor.client.request.accept
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
                addVertex("http://${apps.getPeer("peer0").address}/gmmf/graph/vertex?peerset=peerset0", "user1", Vertex.Type.USER)
            }.isSuccess()

            expectCatching {
                addVertex("http://${apps.getPeer("peer1").address}/gmmf/graph/vertex?peerset=peerset0", "user2", Vertex.Type.USER)
            }.isSuccess()
        }

    @Test
    fun `add edge`(): Unit =
        runBlocking {
            apps = TestApplicationSet(mapOf("peerset0" to listOf("peer0", "peer1")))

            expectCatching {
                addVertex("http://${apps.getPeer("peer0").address}/gmmf/graph/vertex?peerset=peerset0", "user1", Vertex.Type.USER)
            }.isSuccess()

            expectCatching {
                addVertex("http://${apps.getPeer("peer1").address}/gmmf/graph/vertex?peerset=peerset0", "user2", Vertex.Type.USER)
            }.isSuccess()

            expectCatching {
                addEdge(
                    "http://${apps.getPeer("peer0").address}/gmmf/graph/edge?peerset=peerset0",
                    VertexId(ZoneId("peerset0"), "user1"),
                    VertexId(ZoneId("peerset0"), "user2"),
                    Permissions("01010"),
                )
            }.isSuccess()
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
                addVertex("http://${apps.getPeer("peer0").address}/gmmf/graph/vertex?peerset=peerset0", "user1", Vertex.Type.USER)
            }.isSuccess()

            expectCatching {
                addVertex("http://${apps.getPeer("peer2").address}/gmmf/graph/vertex?peerset=peerset1", "user2", Vertex.Type.USER)
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

    private suspend fun addVertex(
        url: String,
        name: String,
        type: Vertex.Type,
    ) {
        testHttpClient.post<HttpResponse>(url) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = NewVertex(name, type)
        }
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
            body = NewEdge(from, to, permissions)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(GmmfModificationSpec::class.java)
    }
}
