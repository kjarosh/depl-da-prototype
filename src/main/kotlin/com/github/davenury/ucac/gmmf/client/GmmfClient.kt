package com.github.davenury.ucac.gmmf.client

import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.gmmf.routing.EdgeMessage
import com.github.davenury.ucac.gmmf.routing.EffectivePermissionsMessage
import com.github.davenury.ucac.gmmf.routing.MembersMessage
import com.github.davenury.ucac.gmmf.routing.ReachesMessage
import com.github.davenury.ucac.gmmf.routing.VertexMessage
import com.github.davenury.ucac.httpClient
import com.github.kjarosh.agh.pp.graph.model.EdgeId
import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.Vertex
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.index.events.Event
import com.github.kjarosh.agh.pp.rest.dto.BulkVertexCreationRequestDto
import io.ktor.client.call.body
import io.ktor.client.request.accept
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import org.slf4j.LoggerFactory

class GmmfClient(val peerResolver: PeerResolver, peer: PeerAddress) {
    private val urlBase: String = "http://${peer.address}"

    suspend fun healthcheck(peerId: PeerId): Boolean {
        val address = peerResolver.resolve(peerId)
        return try {
            val response =
                httpClient.get("http://${address.address}/_meta/health") {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                }.body<HttpResponse>()

            if (response.status == HttpStatusCode.OK) {
                true
            } else {
                logger.info("Non-healthy response: {}, {}", response.status, response)
                false
            }
        } catch (e: Exception) {
            logger.info("Error connecting: {}", e.toString())
            false
        }
    }

    suspend fun addVertex(
        id: VertexId,
        type: Vertex.Type,
    ) {
        httpClient.post("$urlBase/gmmf/graph/vertex?peerset=${id.owner()}") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            setBody(VertexMessage(id.name(), type))
        }.body<HttpResponse>()
    }

    suspend fun addVertices(
        peersetId: PeersetId,
        request: BulkVertexCreationRequestDto,
    ) {
        httpClient.post("$urlBase/gmmf/graph/vertex/bulk?peerset=$peersetId") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            setBody(request)
        }.body<HttpResponse>()
    }

    suspend fun addEdge(
        id: EdgeId,
        permissions: Permissions,
    ) {
        httpClient.post("$urlBase/gmmf/graph/edge?peerset=${id.to.owner()}") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            setBody(EdgeMessage(id.from, id.to, permissions))
        }.body<HttpResponse>()
    }

    suspend fun naiveReaches(
        from: VertexId,
        to: VertexId,
        ttl: Int,
    ): ReachesMessage {
        return httpClient.post("$urlBase/gmmf/naive/reaches?from=$from&to=$to&ttl=$ttl") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.body<ReachesMessage>()
    }

    suspend fun naiveMembers(
        of: VertexId,
        ttl: Int,
    ): MembersMessage {
        return httpClient.post("$urlBase/gmmf/naive/members?of=$of&ttl=$ttl") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.body()
    }

    suspend fun naiveEffectivePermissions(
        from: VertexId,
        to: VertexId,
        ttl: Int,
    ): EffectivePermissionsMessage {
        return httpClient.post("$urlBase/gmmf/naive/effective_permissions?from=$from&to=$to&ttl=$ttl") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.body()
    }

    suspend fun indexedReaches(
        from: VertexId,
        to: VertexId,
    ): ReachesMessage {
        return httpClient.post("$urlBase/gmmf/indexed/reaches?from=$from&to=$to") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.body()
    }

    suspend fun indexedMembers(of: VertexId): MembersMessage {
        return httpClient.post("$urlBase/gmmf/indexed/members?of=$of") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.body()
    }

    suspend fun indexedEffectivePermissions(
        from: VertexId,
        to: VertexId,
    ): EffectivePermissionsMessage {
        return httpClient.post("$urlBase/gmmf/indexed/effective_permissions?from=$from&to=$to") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.body()
    }

    suspend fun sendEvent(
        to: VertexId,
        event: Event,
    ): Boolean {
        return try {
            val response =
                httpClient.post("$urlBase/gmmf/event?to=$to") {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    setBody(event)
                }.body<HttpResponse>()
            response.status == HttpStatusCode.OK
        } catch (e: Exception) {
            logger.info("Error sending event: {}", e.toString())
            false
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger("gmmf-client")
    }
}
