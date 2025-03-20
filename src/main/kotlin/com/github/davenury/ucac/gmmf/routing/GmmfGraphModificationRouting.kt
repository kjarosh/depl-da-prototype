package com.github.davenury.ucac.gmmf.routing

import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.PeersetId
import com.github.davenury.common.StandardChange
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
import kotlinx.coroutines.delay
import kotlinx.coroutines.future.await
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

fun Application.gmmfGraphModificationRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.consensus(): ConsensusProtocol {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).consensusProtocol
    }

    fun ApplicationCall.twoPC(): TwoPC {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).twoPC
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

    suspend fun tryAddEdge(
        call: ApplicationCall,
        from: VertexId,
        to: VertexId,
        permissions: Permissions,
    ): Boolean {
        val fromLocal = from.owner().id == call.peersetId().peersetId
        val toLocal = to.owner().id == call.peersetId().peersetId

        val tx = AddEdgeTx(from, to, permissions, UUID.randomUUID().toString(), UUID.randomUUID().toString())
        val result =
            if (fromLocal && toLocal) {
                val change =
                    StandardChange(
                        tx.serialize(),
                        peersets = listOf(ChangePeersetInfo(call.peersetId(), null)),
                    )
                call.consensus().proposeChangeAsync(change).await()
            } else if (fromLocal || toLocal) {
                val change =
                    StandardChange(
                        tx.serialize(),
                        peersets =
                            listOf(
                                ChangePeersetInfo(PeersetId(from.owner().id), null),
                                ChangePeersetInfo(PeersetId(to.owner().id), null),
                            ),
                    )
                call.twoPC().proposeChangeAsync(change).await()
            } else {
                throw IllegalArgumentException("Trying to add an edge with no vertex in this peerset")
            }

        when (result.status) {
            ChangeResult.Status.ABORTED, ChangeResult.Status.CONFLICT -> {
                // Change conflicted, we can try again
                return false
            }
            else -> {
                result.assertSuccess()
                return true
            }
        }
    }

    suspend fun addEdge(
        call: ApplicationCall,
        from: VertexId,
        to: VertexId,
        permissions: Permissions,
    ) {
        val deadline = Instant.now().plus(60, ChronoUnit.SECONDS)
        var delayMillis = 200.0
        while (Instant.now().isBefore(deadline)) {
            if (tryAddEdge(call, from, to, permissions)) {
                return
            }

            delay(delayMillis.toLong())
            delayMillis *= 1.5
        }

        throw AssertionError("Timed out trying to add edge $from->$to, $permissions")
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

        post("/gmmf/graph/sync") {
            val change =
                StandardChange(
                    "sync",
                    peersets = listOf(ChangePeersetInfo(call.peersetId(), null)),
                )
            call.consensus().proposeChangeAsync(change).await().assertSuccess()
            call.respond(HttpStatusCode.OK, "")
        }

        get("/gmmf/graph/index/ready") {
            val peersetId = call.parameters["peerset"]!!
            val indexFromHistory = multiplePeersetProtocols.forPeerset(PeersetId(peersetId)).indexFromHistory
            val ready = indexFromHistory.isReady()
            call.respond(ready)
        }
    }
}
