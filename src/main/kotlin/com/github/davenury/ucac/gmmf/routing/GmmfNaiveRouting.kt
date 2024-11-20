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
import io.ktor.application.Application
import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.response.respond
import io.ktor.routing.post
import io.ktor.routing.routing

fun Application.gmmfNaiveRouting(
    multiplePeersetProtocols: MultiplePeersetProtocols,
    peerResolver: PeerResolver,
) {
    fun protocols(peersetId: PeersetId): PeersetProtocols = multiplePeersetProtocols.forPeerset(peersetId)

    fun graph(peersetId: PeersetId): Graph = multiplePeersetProtocols.forPeerset(peersetId).graphFromHistory.getGraph()

    fun client(peersetId: PeersetId): GmmfClient {
        val peer = peerResolver.getPeerFromPeerset(peersetId)
        return GmmfClient(peer)
    }

    suspend fun reaches(
        call: ApplicationCall,
        from: String,
        to: String,
        ttl: Int,
    ): Boolean {
        if (ttl <= 0) {
            throw RuntimeException("TTL is zero")
        }
        val newTtl = ttl - 1

        val fromId = VertexId(from)
        val toId = VertexId(to)
        val peersetId = PeersetId.create(fromId.owner().id)
        val graph =
            try {
                graph(peersetId)
            } catch (e: UnknownPeersetException) {
                return client(peersetId).reaches(fromId, toId, newTtl).reaches
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

    fun members(
        call: ApplicationCall,
        of: String,
    ): Collection<String> {
        val ofId = VertexId(of)
        val peersetId = PeersetId.create(ofId.owner().id)
        val graph = graph(peersetId)

        val result: MutableSet<String> = HashSet()

        for (edge in graph.getEdgesByDestination(ofId)) {
            result.add(edge.src().toString())
            try {
                result.addAll(members(call, edge.src().toString()))
            } catch (e: StackOverflowError) {
                throw RuntimeException("Found a cycle")
            }
        }

        return result
    }

    fun effectivePermissions(
        call: ApplicationCall,
        from: String,
        to: String,
    ): String? {
        val fromId = VertexId(from)
        val toId = VertexId(to)
        val peersetId = PeersetId.create(fromId.owner().id)
        val graph = graph(peersetId)
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
                try {
                    val other: String? = effectivePermissions(call, edge.dst().toString(), to)
                    permissions =
                        Permissions.combine(
                            permissions,
                            if (other != null) Permissions(other) else null,
                        )
                } catch (e: StackOverflowError) {
                    throw RuntimeException("Found a cycle")
                }
            }
        }

        return permissions?.toString()
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
            call.respond(members(call, ofId))
        }

        post("/gmmf/naive/effective_permissions") {
            val fromId = call.parameters["from"]!!
            val toId = call.parameters["to"]!!
            call.respond(effectivePermissions(call, fromId, toId) ?: "NONE")
        }
    }
}
