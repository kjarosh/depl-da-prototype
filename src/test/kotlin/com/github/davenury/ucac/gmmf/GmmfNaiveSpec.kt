package com.github.davenury.ucac.gmmf

import com.github.davenury.ucac.gmmf.routing.ReachesMessage
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
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isFalse
import strikt.assertions.isTrue

@ExtendWith(TestLogExtension::class)
class GmmfNaiveSpec : IntegrationTestBase() {
    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "application-integration.conf")
    }

    @Test
    fun `naive reaches basic`(): Unit =
        runBlocking {
            apps = TestApplicationSet(mapOf("peerset0" to listOf("peer0", "peer1")))

            logger.info("Adding v1")
            addVertex("peer0", "peerset0", "v1", Vertex.Type.GROUP)
            logger.info("Adding v2")
            addVertex("peer1", "peerset0", "v2", Vertex.Type.GROUP)
            logger.info("Adding v3")
            addVertex("peer0", "peerset0", "v3", Vertex.Type.GROUP)

            logger.info("Adding v1->v2")
            addEdge(
                "peer0",
                "peerset0",
                VertexId(ZoneId("peerset0"), "v1"),
                VertexId(ZoneId("peerset0"), "v2"),
                Permissions("01010"),
            )
            logger.info("Adding v2->v3")
            addEdge(
                "peer0",
                "peerset0",
                VertexId(ZoneId("peerset0"), "v2"),
                VertexId(ZoneId("peerset0"), "v3"),
                Permissions("01010"),
            )

            val reachesMessage1 =
                naiveReaches(
                    "http://${apps.getPeer("peer0").address}/gmmf/naive/reaches?" +
                        "from=peerset0:v1&to=peerset0:v3",
                )
            expectThat(reachesMessage1.reaches).isTrue()

            val reachesMessage2 =
                naiveReaches(
                    "http://${apps.getPeer("peer0").address}/gmmf/naive/reaches?" +
                        "from=peerset0:v1&to=peerset0:v2",
                )
            expectThat(reachesMessage2.reaches).isTrue()

            val reachesMessage3 =
                naiveReaches(
                    "http://${apps.getPeer("peer0").address}/gmmf/naive/reaches?" +
                        "from=peerset0:v2&to=peerset0:v3",
                )
            expectThat(reachesMessage3.reaches).isTrue()

            val reachesMessage4 =
                naiveReaches(
                    "http://${apps.getPeer("peer0").address}/gmmf/naive/reaches?" +
                        "from=peerset0:v3&to=peerset0:v1",
                )
            expectThat(reachesMessage4.reaches).isFalse()
        }

    @Test
    @Disabled("TODO make parentId optional for 2PC")
    fun `naive reaches two peersets`(): Unit =
        runBlocking {
            apps = TestApplicationSet(mapOf("peerset0" to listOf("peer0", "peer1"), "peerset1" to listOf("peer2", "peer3")))

            addVertex("peer0", "peerset0", "v1", Vertex.Type.GROUP)
            addVertex("peer1", "peerset0", "v2", Vertex.Type.GROUP)
            addVertex("peer2", "peerset1", "v3", Vertex.Type.GROUP)
            addVertex("peer3", "peerset1", "v4", Vertex.Type.GROUP)

            var v2 = getVertex("peer1", "peerset0", "v2")
            expectThat(v2.name).isEqualTo("v2")
            expectThat(v2.type).isEqualTo(Vertex.Type.GROUP)

            v2 = getVertex("peer0", "peerset0", "v2")
            expectThat(v2.name).isEqualTo("v2")
            expectThat(v2.type).isEqualTo(Vertex.Type.GROUP)

            addEdge(
                "peer0",
                "peerset0",
                VertexId(ZoneId("peerset0"), "v1"),
                VertexId(ZoneId("peerset0"), "v2"),
                Permissions("01010"),
            )
            addEdge(
                "peer0",
                "peerset0",
                VertexId(ZoneId("peerset0"), "v2"),
                VertexId(ZoneId("peerset1"), "v3"),
                Permissions("01010"),
            )
            addEdge(
                "peer3",
                "peerset1",
                VertexId(ZoneId("peerset1"), "v3"),
                VertexId(ZoneId("peerset1"), "v4"),
                Permissions("01010"),
            )

            val reachesMessage1 =
                naiveReaches(
                    "http://${apps.getPeer("peer0").address}/gmmf/naive/reaches?" +
                        "from=peerset0:v1&to=peerset1:v4",
                )
            expectThat(reachesMessage1.reaches).isTrue()

            val reachesMessage2 =
                naiveReaches(
                    "http://${apps.getPeer("peer4").address}/gmmf/naive/reaches?" +
                        "from=peerset0:v1&to=peerset1:v4",
                )
            expectThat(reachesMessage2.reaches).isTrue()
        }

    private suspend fun naiveReaches(url: String): ReachesMessage =
        testHttpClient.post<ReachesMessage>(url) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

    companion object {
        private val logger = LoggerFactory.getLogger(GmmfNaiveSpec::class.java)
    }
}