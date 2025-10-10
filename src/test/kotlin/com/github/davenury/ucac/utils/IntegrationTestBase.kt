package com.github.davenury.ucac.utils

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeCreationResponse
import com.github.davenury.ucac.gmmf.routing.DeleteEdgeMessage
import com.github.davenury.ucac.gmmf.routing.EdgeMessage
import com.github.davenury.ucac.gmmf.routing.VertexMessage
import com.github.davenury.ucac.testHttpClient
import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.Vertex
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.rest.dto.BulkVertexCreationRequestDto
import io.ktor.client.call.body
import io.ktor.client.request.accept
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.debug.DebugProbes
import kotlinx.coroutines.delay
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant

/**
 * @author Kamil Jarosz
 */
abstract class IntegrationTestBase {
    lateinit var apps: TestApplicationSet

    @OptIn(ExperimentalCoroutinesApi::class)
    @BeforeEach
    internal fun setUpTestBase() {
        DebugProbes.enableCreationStackTraces = true
        DebugProbes.install()
    }

    @AfterEach
    internal fun tearDown() {
        if (this::apps.isInitialized) {
            apps.stopApps()
        }
    }

    suspend fun sync(
        peerName: String,
        peerset: String,
    ) {
        logger.info("Syncing $peerset through $peerName")
        testHttpClient.post("http://${apps.getPeer(peerName).address}/gmmf/graph/sync?peerset=$peerset") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            setBody("test")
        }.body<HttpResponse>()
    }

    suspend fun waitForIndex(
        peerName: String,
        peerset: String,
    ) {
        logger.info("Waiting for index of $peerset through $peerName")
        val start = Instant.now()
        val deadline = start.plus(Duration.ofSeconds(60))
        while (Instant.now() < deadline) {
            if (indexReady(peerName, peerset)) {
                return
            }

            delay(1000)
        }

        throw RuntimeException("Waiting for index timed out")
    }

    suspend fun indexReady(
        peerName: String,
        peerset: String,
    ): Boolean {
        logger.info("Checking index ready of $peerset through $peerName")
        return testHttpClient.get("http://${apps.getPeer(peerName).address}/gmmf/graph/index/ready?peerset=$peerset") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.body()
    }

    suspend fun addVertex(
        peerName: String,
        peerset: String,
        name: String,
        type: Vertex.Type,
    ) {
        logger.info("Adding vertex $name:$type to $peerset through $peerName")
        testHttpClient.post("http://${apps.getPeer(peerName).address}/gmmf/graph/vertex?peerset=$peerset") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            setBody(VertexMessage(name, type))
        }.body<HttpResponse>()
    }

    suspend fun addVertices(
        peerName: String,
        peerset: String,
        vertices: BulkVertexCreationRequestDto,
    ) {
        logger.info("Adding ${vertices.vertices.size} vertices to $peerset through $peerName")
        testHttpClient.post("http://${apps.getPeer(peerName).address}/gmmf/graph/vertex/bulk?peerset=$peerset") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            setBody(vertices)
        }.body<HttpResponse>()
    }

    suspend fun getVertex(
        peerName: String,
        peerset: String,
        name: String,
    ): VertexMessage {
        logger.info("Getting vertex $name from $peerset through $peerName")
        return testHttpClient.get("http://${apps.getPeer(peerName).address}/gmmf/graph/vertex/$name?peerset=$peerset") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.body()
    }

    suspend fun addEdge(
        peerName: String,
        peerset: String,
        from: VertexId,
        to: VertexId,
        permissions: Permissions,
    ) {
        logger.info("Adding edge $from->$to ($permissions) to $peerset through $peerName")
        testHttpClient.post("http://${apps.getPeer(peerName).address}/gmmf/graph/edge?peerset=$peerset") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            setBody(EdgeMessage(from, to, permissions))
        }.body<HttpResponse>()
    }

    suspend fun deleteEdge(
        peerName: String,
        peerset: String,
        from: VertexId,
        to: VertexId,
    ) {
        logger.info("Deleting edge $from->$to to $peerset through $peerName")
        testHttpClient.post("http://${apps.getPeer(peerName).address}/gmmf/graph/edge/delete?peerset=$peerset") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            setBody(DeleteEdgeMessage(from, to))
        }.body<HttpResponse>()
    }

    suspend fun getEdge(
        peerName: String,
        peerset: String,
        from: VertexId,
        to: VertexId,
    ): EdgeMessage {
        logger.info("Getting edge $from->$to from $peerset through $peerName")
        return testHttpClient.get("http://${apps.getPeer(peerName).address}/gmmf/graph/edge/$from/$to?peerset=$peerset") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.body()
    }

    suspend fun executeChangeSync(
        peerName: String,
        peerset: String,
        change: Change,
        use2pc: Boolean = false,
    ): ChangeCreationResponse {
        var url = "http://${apps.getPeer(peerName).address}/v2/change/sync?peerset=$peerset"
        if (use2pc) {
            url += "&use_2pc=true"
        }
        logger.info("Executing sync change $change from $peerset through $peerName")
        return testHttpClient.post(url) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            setBody(change)
        }.body()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(IntegrationTestBase::class.java)
    }
}
