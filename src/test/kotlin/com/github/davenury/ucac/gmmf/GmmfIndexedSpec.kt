package com.github.davenury.ucac.gmmf

import com.github.davenury.ucac.gmmf.routing.EffectivePermissionsMessage
import com.github.davenury.ucac.gmmf.routing.MembersMessage
import com.github.davenury.ucac.gmmf.routing.ReachesMessage
import com.github.davenury.ucac.testHttpClient
import com.github.davenury.ucac.utils.IntegrationTestBase
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.davenury.ucac.utils.eventuallyBlocking
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
import strikt.assertions.isNull
import strikt.assertions.isTrue

@ExtendWith(TestLogExtension::class)
class GmmfIndexedSpec : IntegrationTestBase() {
    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "application-integration.conf")
    }

    @Test
    fun `indexed reaches basic`(): Unit =
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

            eventuallyBlocking(30) {
                val reachesMessage1 =
                    indexedReaches(
                        "http://${apps.getPeer("peer0").address}/gmmf/indexed/reaches?" +
                            "from=peerset0:v1&to=peerset0:v3",
                    )
                expectThat(reachesMessage1.reaches).isTrue()

                val reachesMessage2 =
                    indexedReaches(
                        "http://${apps.getPeer("peer0").address}/gmmf/indexed/reaches?" +
                            "from=peerset0:v1&to=peerset0:v2",
                    )
                expectThat(reachesMessage2.reaches).isTrue()

                val reachesMessage3 =
                    indexedReaches(
                        "http://${apps.getPeer("peer0").address}/gmmf/indexed/reaches?" +
                            "from=peerset0:v2&to=peerset0:v3",
                    )
                expectThat(reachesMessage3.reaches).isTrue()

                val reachesMessage4 =
                    indexedReaches(
                        "http://${apps.getPeer("peer0").address}/gmmf/indexed/reaches?" +
                            "from=peerset0:v3&to=peerset0:v1",
                    )
                expectThat(reachesMessage4.reaches).isFalse()
            }
        }

    @Test
    fun `indexed reaches two peersets`(): Unit =
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

            eventuallyBlocking(30) {
                val reachesMessage1 =
                    indexedReaches(
                        "http://${apps.getPeer("peer0").address}/gmmf/indexed/reaches?" +
                            "from=peerset0:v1&to=peerset1:v4",
                    )
                expectThat(reachesMessage1.reaches).isTrue()

                val reachesMessage2 =
                    indexedReaches(
                        "http://${apps.getPeer("peer3").address}/gmmf/indexed/reaches?" +
                            "from=peerset0:v1&to=peerset1:v4",
                    )
                expectThat(reachesMessage2.reaches).isTrue()
            }
        }

    @Test
    fun `indexed members basic`(): Unit =
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

            eventuallyBlocking(30) {
                val membersMessage1 =
                    indexedMembers(
                        "http://${apps.getPeer("peer0").address}/gmmf/indexed/members?" +
                            "of=peerset0:v1",
                    )
                expectThat(membersMessage1.members).isEqualTo(setOf())

                val membersMessage2 =
                    indexedMembers(
                        "http://${apps.getPeer("peer0").address}/gmmf/indexed/members?" +
                            "of=peerset0:v2",
                    )
                expectThat(membersMessage2.members).isEqualTo(setOf(VertexId("peerset0:v1")))

                val membersMessage3 =
                    indexedMembers(
                        "http://${apps.getPeer("peer0").address}/gmmf/indexed/members?" +
                            "of=peerset0:v3",
                    )
                expectThat(membersMessage3.members).isEqualTo(
                    setOf(
                        VertexId("peerset0:v1"),
                        VertexId("peerset0:v2"),
                    ),
                )
            }
        }

    @Test
    fun `indexed members two peersets`(): Unit =
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

            eventuallyBlocking(30) {
                val membersMessage1 =
                    indexedMembers(
                        "http://${apps.getPeer("peer0").address}/gmmf/indexed/members?" +
                            "of=peerset0:v1",
                    )
                expectThat(membersMessage1.members).isEqualTo(setOf())

                val membersMessage2 =
                    indexedMembers(
                        "http://${apps.getPeer("peer3").address}/gmmf/indexed/members?" +
                            "of=peerset1:v4",
                    )
                expectThat(membersMessage2.members).isEqualTo(
                    setOf(
                        VertexId("peerset0:v1"),
                        VertexId("peerset0:v2"),
                        VertexId("peerset1:v3"),
                    ),
                )
            }
        }

    /**
     *    v1
     *   ↙  ↘
     * v2a  v2b
     *   ↘  ↙
     *    v3
     */
    @Test
    fun `indexed effective permissions basic`(): Unit =
        runBlocking {
            apps = TestApplicationSet(mapOf("peerset0" to listOf("peer0", "peer1")))

            logger.info("Adding v1")
            addVertex("peer0", "peerset0", "v1", Vertex.Type.GROUP)
            logger.info("Adding v2a")
            addVertex("peer1", "peerset0", "v2a", Vertex.Type.GROUP)
            logger.info("Adding v2b")
            addVertex("peer1", "peerset0", "v2b", Vertex.Type.GROUP)
            logger.info("Adding v3")
            addVertex("peer0", "peerset0", "v3", Vertex.Type.GROUP)

            logger.info("Adding v1->v2a")
            addEdge(
                "peer0",
                "peerset0",
                VertexId(ZoneId("peerset0"), "v1"),
                VertexId(ZoneId("peerset0"), "v2a"),
                Permissions("10000"),
            )
            logger.info("Adding v1->v2b")
            addEdge(
                "peer0",
                "peerset0",
                VertexId(ZoneId("peerset0"), "v1"),
                VertexId(ZoneId("peerset0"), "v2b"),
                Permissions("01000"),
            )
            logger.info("Adding v2a->v3")
            addEdge(
                "peer0",
                "peerset0",
                VertexId(ZoneId("peerset0"), "v2a"),
                VertexId(ZoneId("peerset0"), "v3"),
                Permissions("00100"),
            )
            logger.info("Adding v2b->v3")
            addEdge(
                "peer0",
                "peerset0",
                VertexId(ZoneId("peerset0"), "v2b"),
                VertexId(ZoneId("peerset0"), "v3"),
                Permissions("00010"),
            )

            eventuallyBlocking(30) {
                val epMessage1 =
                    indexedEffectivePermissions(
                        "http://${apps.getPeer("peer0").address}/gmmf/indexed/effective_permissions?" +
                            "from=peerset0:v1&to=peerset0:v3",
                    )
                expectThat(epMessage1.effectivePermissions).isEqualTo(Permissions("00110"))

                val epMessage2 =
                    indexedEffectivePermissions(
                        "http://${apps.getPeer("peer0").address}/gmmf/indexed/effective_permissions?" +
                            "from=peerset0:v3&to=peerset0:v1",
                    )
                expectThat(epMessage2.effectivePermissions).isNull()
            }
        }

    /**
     *    v1
     *   ↙  ↘
     * v2a  v2b
     *   ↘  ↙
     *    v3
     */
    @Test
    @Disabled
    fun `indexed effective permissions multiple peersets`(): Unit =
        runBlocking {
            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2"),
                        "peerset1" to listOf("peer3", "peer4"),
                        "peerset2" to listOf("peer5", "peer6", "peer7"),
                    ),
                )

            logger.info("Adding v1")
            addVertex("peer0", "peerset0", "v1", Vertex.Type.GROUP)
            logger.info("Adding v2a")
            addVertex("peer3", "peerset1", "v2a", Vertex.Type.GROUP)
            logger.info("Adding v2b")
            addVertex("peer3", "peerset1", "v2b", Vertex.Type.GROUP)
            logger.info("Adding v3")
            addVertex("peer7", "peerset2", "v3", Vertex.Type.GROUP)

            logger.info("Adding v1->v2a")
            addEdge(
                "peer3",
                "peerset1",
                VertexId(ZoneId("peerset0"), "v1"),
                VertexId(ZoneId("peerset1"), "v2a"),
                Permissions("10000"),
            )
            logger.info("Adding v1->v2b")
            addEdge(
                "peer3",
                "peerset1",
                VertexId(ZoneId("peerset0"), "v1"),
                VertexId(ZoneId("peerset1"), "v2b"),
                Permissions("01000"),
            )
            logger.info("Adding v2a->v3")
            addEdge(
                "peer5",
                "peerset2",
                VertexId(ZoneId("peerset1"), "v2a"),
                VertexId(ZoneId("peerset2"), "v3"),
                Permissions("00100"),
            )
            logger.info("Adding v2b->v3")
            addEdge(
                "peer5",
                "peerset2",
                VertexId(ZoneId("peerset1"), "v2b"),
                VertexId(ZoneId("peerset2"), "v3"),
                Permissions("00010"),
            )

            eventuallyBlocking(30) {
                val epMessage1 =
                    indexedEffectivePermissions(
                        "http://${apps.getPeer("peer0").address}/gmmf/indexed/effective_permissions?" +
                            "from=peerset0:v1&to=peerset2:v3",
                    )
                expectThat(epMessage1.effectivePermissions).isEqualTo(Permissions("00110"))

                val epMessage2 =
                    indexedEffectivePermissions(
                        "http://${apps.getPeer("peer0").address}/gmmf/indexed/effective_permissions?" +
                            "from=peerset2:v3&to=peerset0:v1",
                    )
                expectThat(epMessage2.effectivePermissions).isNull()
            }
        }

    private suspend fun indexedReaches(url: String): ReachesMessage =
        testHttpClient.post<ReachesMessage>(url) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

    private suspend fun indexedMembers(url: String): MembersMessage =
        testHttpClient.post<MembersMessage>(url) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

    private suspend fun indexedEffectivePermissions(url: String): EffectivePermissionsMessage =
        testHttpClient.post<EffectivePermissionsMessage>(url) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

    companion object {
        private val logger = LoggerFactory.getLogger(GmmfIndexedSpec::class.java)
    }
}
