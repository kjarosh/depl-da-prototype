package com.github.davenury.ucac.gmmf.routing

import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.PeersetId
import com.github.davenury.common.StandardChange
import com.github.davenury.common.UnknownPeersetException
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.gmmf.client.GmmfClient
import com.github.davenury.ucac.gmmf.model.AcceptExternalEvent
import com.github.davenury.ucac.gmmf.model.IndexFromHistory
import com.github.kjarosh.agh.pp.graph.model.EdgeId
import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.index.EffectiveVertex
import com.github.kjarosh.agh.pp.index.VertexIndices
import com.github.kjarosh.agh.pp.index.events.Event
import io.ktor.application.Application
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.routing.get
import io.ktor.routing.post
import io.ktor.routing.routing
import kotlinx.coroutines.future.await

fun Application.gmmfIndexedRouting(
    multiplePeersetProtocols: MultiplePeersetProtocols,
    peerResolver: PeerResolver,
) {
    fun indexFromHistory(peersetId: PeersetId): IndexFromHistory = multiplePeersetProtocols.forPeerset(peersetId).indexFromHistory

    fun indices(peersetId: PeersetId): VertexIndices = indexFromHistory(peersetId).getIndices()

    fun client(peersetId: PeersetId): GmmfClient {
        val peer = peerResolver.getPeerFromPeerset(peersetId)
        return GmmfClient(peerResolver, peer)
    }

    suspend fun members(of: String): Set<VertexId> {
        val ofId = VertexId(of)
        val peersetId = PeersetId.create(ofId.owner().id)
        return try {
            indices(peersetId).getIndexOf(ofId).effectiveChildrenSet
        } catch (e: UnknownPeersetException) {
            client(peersetId).indexedMembers(ofId).members
        }
    }

    suspend fun effectivePermissions(
        from: String,
        to: String,
    ): Permissions? {
        val edgeId =
            EdgeId.of(
                VertexId(from),
                VertexId(to),
            )
        val peersetId = PeersetId.create(edgeId.to.owner().id)

        return try {
            indices(peersetId).getIndexOf(edgeId.to)
                .getEffectiveChild(edgeId.from)
                .map { obj: EffectiveVertex -> obj.effectivePermissions }
                .orElse(null)
        } catch (e: UnknownPeersetException) {
            client(peersetId).indexedEffectivePermissions(edgeId.from, edgeId.to).effectivePermissions
        }
    }

    suspend fun reaches(
        from: String,
        to: String,
    ): Boolean {
        return effectivePermissions(from, to) != null
    }

    routing {
        post("/gmmf/indexed/reaches") {
            val fromId = call.parameters["from"]!!
            val toId = call.parameters["to"]!!
            call.respond(ReachesMessage(reaches(fromId, toId)))
        }

        post("/gmmf/indexed/members") {
            val ofId = call.parameters["of"]!!
            call.respond(MembersMessage(members(ofId)))
        }

        post("/gmmf/indexed/effective_permissions") {
            val fromId = call.parameters["from"]!!
            val toId = call.parameters["to"]!!
            call.respond(EffectivePermissionsMessage(effectivePermissions(fromId, toId)))
        }

        post("/gmmf/event") {
            val vertexId = VertexId(call.parameters["to"]!!)
            val peersetId = PeersetId(vertexId.owner().id)
            val event = call.receive<Event>()

            val tx = AcceptExternalEvent(vertexId, event)
            val change =
                StandardChange(
                    tx.serialize(),
                    // TODO parent id?
                    peersets = listOf(ChangePeersetInfo(peersetId, null)),
                )
            val changeResult =
                multiplePeersetProtocols.forPeerset(peersetId)
                    .consensusProtocol
                    .proposeChangeAsync(change)
                    .await()
            if (changeResult.status == ChangeResult.Status.SUCCESS) {
                call.respond(HttpStatusCode.OK)
            } else {
                call.respond(HttpStatusCode.InternalServerError)
            }
        }
    }
}
