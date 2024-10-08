package com.github.davenury.ucac.utils

import com.github.davenury.ucac.gmmf.routing.EdgeMessage
import com.github.davenury.ucac.gmmf.routing.VertexMessage
import com.github.davenury.ucac.testHttpClient
import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.Vertex
import com.github.kjarosh.agh.pp.graph.model.VertexId
import io.ktor.client.request.accept
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.statement.HttpResponse
import io.ktor.http.ContentType
import io.ktor.http.contentType
import org.junit.jupiter.api.AfterEach
import org.slf4j.LoggerFactory

/**
 * @author Kamil Jarosz
 */
abstract class IntegrationTestBase {
    lateinit var apps: TestApplicationSet

    @AfterEach
    internal fun tearDown() {
        if (this::apps.isInitialized) {
            apps.stopApps()
        }
    }

    suspend fun addVertex(
        peerName: String,
        peerset: String,
        name: String,
        type: Vertex.Type,
    ) {
        logger.info("Adding vertex $name:$type to $peerset through $peerName")
        testHttpClient.post<HttpResponse>("http://${apps.getPeer(peerName).address}/gmmf/graph/vertex?peerset=$peerset") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = VertexMessage(name, type)
        }
    }

    suspend fun getVertex(
        peerName: String,
        peerset: String,
        name: String,
    ): VertexMessage {
        logger.info("Getting vertex $name from $peerset through $peerName")
        return testHttpClient.get<VertexMessage>("http://${apps.getPeer(peerName).address}/gmmf/graph/vertex/$name?peerset=$peerset") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
    }

    suspend fun addEdge(
        peerName: String,
        peerset: String,
        from: VertexId,
        to: VertexId,
        permissions: Permissions,
    ) {
        logger.info("Adding edge $from->$to ($permissions) to $peerset through $peerName")
        testHttpClient.post<HttpResponse>("http://${apps.getPeer(peerName).address}/gmmf/graph/edge?peerset=$peerset") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = EdgeMessage(from, to, permissions)
        }
    }

    suspend fun getEdge(
        peerName: String,
        peerset: String,
        from: VertexId,
        to: VertexId,
    ): EdgeMessage {
        logger.info("Getting edge $from->$to from $peerset through $peerName")
        return testHttpClient.get<EdgeMessage>("http://${apps.getPeer(peerName).address}/gmmf/graph/edge/$from/$to?peerset=$peerset") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(IntegrationTestBase::class.java)
    }
}
