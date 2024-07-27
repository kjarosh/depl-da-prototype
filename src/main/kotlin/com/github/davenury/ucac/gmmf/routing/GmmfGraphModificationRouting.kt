package com.github.davenury.ucac.gmmf.routing

import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.PeersetId
import com.github.davenury.common.StandardChange
import com.github.davenury.common.history.History
import com.github.davenury.common.peersetId
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.davenury.ucac.consensus.ConsensusProtocol
import com.github.davenury.ucac.gmmf.model.AddExternalEdgeTx
import com.github.davenury.ucac.gmmf.model.AddLocalEdgeTx
import com.github.davenury.ucac.gmmf.model.AddVertexTx
import com.github.kjarosh.agh.pp.graph.GraphLoader
import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.Vertex
import com.github.kjarosh.agh.pp.graph.model.VertexId
import io.ktor.application.Application
import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.request.receive
import io.ktor.response.respond
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

    val graphLoader = GraphLoader()
    graphLoader.init()

    suspend fun addVertex(
        call: ApplicationCall,
        name: String,
        type: Vertex.Type,
    ) {
        val parentId = call.history().getCurrentEntryId()
        val tx = AddVertexTx(name, type)
        val change =
            StandardChange(
                tx.serialize(),
                peersets = listOf(ChangePeersetInfo(call.peersetId(), parentId)),
            )
        call.consensus().proposeChangeAsync(change).await()
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
            val tx = AddLocalEdgeTx(from.name(), to.name(), permissions)
            val change =
                StandardChange(
                    tx.serialize(),
                    peersets = listOf(ChangePeersetInfo(call.peersetId(), parentId)),
                )
            call.consensus().proposeChangeAsync(change).await()
        } else if (fromLocal || toLocal) {
            val tx = AddExternalEdgeTx(from, to, permissions)
            val change =
                StandardChange(
                    tx.serialize(),
                    peersets =
                        listOf(
                            ChangePeersetInfo(PeersetId(from.owner().id), fromParentId),
                            ChangePeersetInfo(PeersetId(to.owner().id), toParentId),
                        ),
                )
            call.twoPC().proposeChangeAsync(change).await()
        } else {
            throw IllegalArgumentException("Trying to add an edge with no vertex in this peerset")
        }
    }

    routing {
        post("/gmmf/graph/vertex") {
            val request = call.receive<NewVertex>()
            addVertex(call, request.name, request.type)
            call.respond(true)
        }

        post("/gmmf/graph/edge") {
            val request = call.receive<NewEdge>()
            addEdge(call, request.from, request.to, request.permissions)
            call.respond(true)
        }
    }
}
