package com.github.davenury.ucac.gmmf.routing

import com.github.davenury.common.PeersetId
import com.github.davenury.common.UnknownPeersetException
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.PeersetProtocols
import com.github.davenury.ucac.gmmf.client.GmmfClient
import com.github.davenury.ucac.gmmf.service.BasicQueriesService
import com.github.kjarosh.agh.pp.graph.model.EdgeId
import com.github.kjarosh.agh.pp.graph.model.Graph
import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.VertexId
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.call
import io.ktor.server.response.respond
import io.ktor.server.routing.post
import io.ktor.server.routing.routing

fun Application.gmmfNaiveRouting(
    multiplePeersetProtocols: MultiplePeersetProtocols,
    peerResolver: PeerResolver,
) {
    fun protocols(peersetId: PeersetId): PeersetProtocols = multiplePeersetProtocols.forPeerset(peersetId)

    fun graph(peersetId: PeersetId): Graph = multiplePeersetProtocols.forPeerset(peersetId).graphFromHistory.getGraph()

    fun client(peersetId: PeersetId): GmmfClient {
        val peer = peerResolver.getPeerFromPeerset(peersetId)
        return GmmfClient(peerResolver, peer)
    }

    fun newTtl(ttl: Int): Int {
        if (ttl <= 0) {
            throw RuntimeException("TTL is zero")
        }
        return ttl - 1
    }

    suspend fun reaches(
        call: ApplicationCall,
        from: String,
        to: String,
        ttl: Int,
    ): Boolean {
        val newTtl = newTtl(ttl)

        val fromId = VertexId(from)
        val toId = VertexId(to)
        val peersetId = PeersetId.create(fromId.owner().id)
        val graph =
            try {
                graph(peersetId)
            } catch (e: UnknownPeersetException) {
                return client(peersetId).naiveReaches(fromId, toId, newTtl).reaches
            }
        val edgeId: EdgeId = EdgeId.of(fromId, toId)

        if (BasicQueriesService(protocols(peersetId)).isAdjacent(fromId, toId)) {
            return true
        }

        for (edge in graph
            .getEdgesBySource(edgeId.getFrom())
            .stream()) {
            if (reaches(call, edge.dst().toString(), to, newTtl)) {
                return true
            }
        }

        return false
    }

    suspend fun members(
        call: ApplicationCall,
        of: String,
        ttl: Int,
    ): Set<VertexId> {
        val newTtl = newTtl(ttl)

        val ofId = VertexId(of)
        val peersetId = PeersetId.create(ofId.owner().id)
        val graph =
            try {
                graph(peersetId)
            } catch (e: UnknownPeersetException) {
                return client(peersetId).naiveMembers(ofId, newTtl).members
            }

        val result: MutableSet<VertexId> = HashSet()

        for (edge in graph.getEdgesByDestination(ofId)) {
            result.add(edge.src())
            result.addAll(members(call, edge.src().toString(), newTtl))
        }

        return result
    }

    suspend fun effectivePermissions(
        call: ApplicationCall,
        from: String,
        to: String,
        ttl: Int,
    ): Permissions? {
        val newTtl = newTtl(ttl)

        val fromId = VertexId(from)
        val toId = VertexId(to)
        val peersetId = PeersetId.create(fromId.owner().id)
        val graph =
            try {
                graph(peersetId)
            } catch (e: UnknownPeersetException) {
                return client(peersetId).naiveEffectivePermissions(fromId, toId, newTtl).effectivePermissions
            }
        val edgeId: EdgeId = EdgeId.of(fromId, toId)

        var permissions: Permissions? = null

        for (edge in graph.getEdgesBySource(edgeId.getFrom())) {
            if (edge.dst().equals(edgeId.getTo())) {
                permissions =
                    Permissions.combine(
                        permissions,
                        edge.permissions(),
                    )
            } else {
                val other: Permissions? = effectivePermissions(call, edge.dst().toString(), to, newTtl)
                permissions =
                    Permissions.combine(
                        permissions,
                        other,
                    )
            }
        }

        return permissions
    }

    routing {
        post("/gmmf/naive/reaches") {
            val fromId = call.parameters["from"]!!
            val toId = call.parameters["to"]!!
            val ttl = (call.parameters["ttl"] ?: "128").toInt()
            call.respond(ReachesMessage(reaches(call, fromId, toId, ttl)))
        }

        post("/gmmf/naive/members") {
            val ofId = call.parameters["of"]!!
            val ttl = (call.parameters["ttl"] ?: "128").toInt()
            call.respond(MembersMessage(members(call, ofId, ttl)))
        }

        post("/gmmf/naive/effective_permissions") {
            val fromId = call.parameters["from"]!!
            val toId = call.parameters["to"]!!
            val ttl = (call.parameters["ttl"] ?: "128").toInt()
            call.respond(EffectivePermissionsMessage(effectivePermissions(call, fromId, toId, ttl)))
        }
    }
}
