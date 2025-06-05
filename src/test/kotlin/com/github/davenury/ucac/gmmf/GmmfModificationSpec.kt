package com.github.davenury.ucac.gmmf

import com.github.davenury.ucac.utils.IntegrationTestBase
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.Vertex
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.graph.model.ZoneId
import com.github.kjarosh.agh.pp.rest.dto.BulkVertexCreationRequestDto
import com.github.kjarosh.agh.pp.rest.dto.VertexCreationRequestDto
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junitpioneer.jupiter.RetryingTest
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

            sync("peer0", "peerset0")

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
    fun `add vertices`(): Unit =
        runBlocking {
            apps = TestApplicationSet(mapOf("peerset0" to listOf("peer0", "peer1")))

            expectCatching {
                addVertices(
                    "peer0",
                    "peerset0",
                    BulkVertexCreationRequestDto(
                        listOf(
                            VertexCreationRequestDto("user1", Vertex.Type.USER),
                            VertexCreationRequestDto("user2", Vertex.Type.USER),
                        ),
                    ),
                )
            }.isSuccess()

            sync("peer1", "peerset0")

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

            sync("peer0", "peerset0")

            val vertexUser1 = VertexId(ZoneId("peerset0"), "user1")
            val vertexUser2 = VertexId(ZoneId("peerset0"), "user2")
            expectCatching {
                addEdge(
                    "peer0",
                    "peerset0",
                    vertexUser1,
                    vertexUser2,
                    Permissions("01010"),
                )
            }.isSuccess()

            val edgePeer0 = getEdge("peer0", "peerset0", vertexUser1, vertexUser2)
            val edgePeer1 = getEdge("peer1", "peerset0", vertexUser1, vertexUser2)

            expectThat(edgePeer0.permissions).isEqualTo(Permissions("01010"))
            expectThat(edgePeer1.permissions).isEqualTo(Permissions("01010"))
        }

    @RetryingTest(3)
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
                addVertex("peer0", "peerset0", "user1b", Vertex.Type.USER)
                addVertex("peer2", "peerset1", "user2", Vertex.Type.USER)
                addVertex("peer2", "peerset1", "user2b", Vertex.Type.USER)
            }.isSuccess()

            val user1 = VertexId(ZoneId("peerset0"), "user1")
            val user2 = VertexId(ZoneId("peerset1"), "user2")
            expectCatching {
                addEdge(
                    "peer0",
                    "peerset0",
                    user1,
                    user2,
                    Permissions("01010"),
                )
            }.isSuccess()

            sync("peer0", "peerset0")
            sync("peer1", "peerset0")
            sync("peer3", "peerset1")

            expectCatching {
                addEdge(
                    "peer0",
                    "peerset0",
                    VertexId(ZoneId("peerset0"), "user1b"),
                    VertexId(ZoneId("peerset1"), "user2b"),
                    Permissions("11011"),
                )
            }.isSuccess()

            sync("peer1", "peerset0")
            sync("peer2", "peerset1")
            sync("peer3", "peerset1")

            val edge1 = getEdge("peer0", "peerset0", user1, user2)
            val edge2 = getEdge("peer1", "peerset0", user1, user2)
            val edge3 = getEdge("peer2", "peerset1", user1, user2)
            val edge4 = getEdge("peer3", "peerset1", user1, user2)

            expectThat(edge1.permissions).isEqualTo(Permissions("01010"))
            expectThat(edge2.permissions).isEqualTo(Permissions("01010"))
            expectThat(edge3.permissions).isEqualTo(Permissions("01010"))
            expectThat(edge4.permissions).isEqualTo(Permissions("01010"))
        }

    companion object {
        private val logger = LoggerFactory.getLogger(GmmfModificationSpec::class.java)
    }
}
