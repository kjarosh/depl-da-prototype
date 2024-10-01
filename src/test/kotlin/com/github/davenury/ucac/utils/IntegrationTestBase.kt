package com.github.davenury.ucac.utils

import com.github.davenury.ucac.gmmf.routing.VertexMessage
import com.github.davenury.ucac.testHttpClient
import com.github.kjarosh.agh.pp.graph.model.Vertex
import io.ktor.client.request.accept
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

    companion object {
        private val logger = LoggerFactory.getLogger(IntegrationTestBase::class.java)
    }
}
