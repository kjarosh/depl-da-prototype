package com.github.davenury.ucac.gmmf.routing

import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.PeersetId
import com.github.davenury.common.StandardChange
import com.github.davenury.common.history.History
import com.github.davenury.common.peersetId
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.github.davenury.ucac.gmmf.model.AddEdgeTx
import com.github.davenury.ucac.gmmf.model.AddVertexTx
import com.github.kjarosh.agh.pp.graph.model.EdgeId
import com.github.kjarosh.agh.pp.graph.model.Graph
import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.Vertex
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.graph.model.ZoneId
import com.github.kjarosh.agh.pp.rest.dto.BulkVertexCreationRequestDto
import io.ktor.application.Application
import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.routing.get
import io.ktor.routing.post
import io.ktor.routing.routing
import kotlinx.coroutines.future.await

fun Application.gmmfGraphModificationRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.consensus(): ConsensusProtocol {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).consensusProtocol
    }

    fun ApplicationCall.twoPC(): TwoPC {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).twoPC
    }

    fun ApplicationCall.history(): History {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).history
    }

    fun ApplicationCall.graph(): Graph {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).graphFromHistory.getGraph()
    }

    suspend fun addVertex(
        call: ApplicationCall,
        name: String,
        type: Vertex.Type,
    ) {
        val vertexId = VertexId(ZoneId(call.peersetId().peersetId), name)
        val tx = AddVertexTx(vertexId, type)
        val change =
            StandardChange(
                tx.serialize(),
                peersets = listOf(ChangePeersetInfo(call.peersetId(), null)),
            )
        call.consensus().proposeChangeAsync(change).await().assertSuccess()
    }

    fun getVertex(
        call: ApplicationCall,
        name: String,
    ): VertexMessage? {
        val vertexId = VertexId(ZoneId(call.peersetId().peersetId), name)
        val vertex = call.graph().getVertex(vertexId)
        return vertex?.let {
            VertexMessage(it.id().name(), it.type())
        }
    }

    suspend fun addEdge(
        call: ApplicationCall,
        from: VertexId,
        to: VertexId,
        permissions: Permissions,
    ) {
        val parentId = call.history().getCurrentEntryId()

        val fromLocal = from.owner().id == call.peersetId().peersetId
        val toLocal = to.owner().id == call.peersetId().peersetId

        val fromParentId =
            if (from.owner().id == call.peersetId().peersetId) {
                parentId
            } else {
                null
            }
        val toParentId =
            if (to.owner().id == call.peersetId().peersetId) {
                parentId
            } else {
                null
            }

        if (fromLocal && toLocal) {
            val tx = AddEdgeTx(from, to, permissions)
            val change =
                StandardChange(
                    tx.serialize(),
                    peersets = listOf(ChangePeersetInfo(call.peersetId(), parentId)),
                )
            call.consensus().proposeChangeAsync(change).await().assertSuccess()
        } else if (fromLocal || toLocal) {
            val tx = AddEdgeTx(from, to, permissions)
            val change =
                StandardChange(
                    tx.serialize(),
                    peersets =
                        listOf(
                            ChangePeersetInfo(PeersetId(from.owner().id), fromParentId),
                            ChangePeersetInfo(PeersetId(to.owner().id), toParentId),
                        ),
                )
            call.twoPC().proposeChangeAsync(change).await().assertSuccess()
        } else {
            throw IllegalArgumentException("Trying to add an edge with no vertex in this peerset")
        }
    }

    fun getEdge(
        call: ApplicationCall,
        from: String,
        to: String,
    ): EdgeMessage? {
        val zoneId = ZoneId(call.peersetId().peersetId)
        val fromId = if (from.contains(":")) VertexId(from) else VertexId(zoneId, from)
        val toId = if (from.contains(":")) VertexId(to) else VertexId(zoneId, to)

        val edge = call.graph().getEdge(EdgeId(fromId, toId))
        return edge?.let {
            EdgeMessage(it.id().from, it.id().to, it.permissions())
        }
    }

    routing {
        post("/gmmf/graph/vertex") {
            val request = call.receive<VertexMessage>()
            addVertex(call, request.name, request.type)
            call.respond("")
        }

        post("/gmmf/graph/vertex/bulk") {
            val request = call.receive<BulkVertexCreationRequestDto>()
            for (vertex in request.vertices) {
                addVertex(call, vertex.name, vertex.type)
            }
            call.respond("")
        }

        get("/gmmf/graph/vertex/{name}") {
            val name = call.parameters["name"]!!

            getVertex(call, name)?.also {
                call.respond(it)
            } ?: call.respond(HttpStatusCode.NotFound, "")
        }

        post("/gmmf/graph/edge") {
            val request = call.receive<EdgeMessage>()
            addEdge(call, request.from, request.to, request.permissions)
            call.respond("")
        }

        get("/gmmf/graph/edge/{from}/{to}") {
            val from = call.parameters["from"]!!
            val to = call.parameters["to"]!!

            getEdge(call, from, to)?.also {
                call.respond(it)
            } ?: call.respond(HttpStatusCode.NotFound, "")
        }
    }
}
