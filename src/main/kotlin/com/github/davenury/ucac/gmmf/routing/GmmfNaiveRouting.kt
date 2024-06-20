package com.github.davenury.ucac.gmmf.routing

import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.kjarosh.agh.pp.config.Config.ZONE_ID
import com.github.kjarosh.agh.pp.graph.GraphLoader
import com.github.kjarosh.agh.pp.graph.model.EdgeId
import com.github.kjarosh.agh.pp.graph.model.Graph
import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.graph.model.ZoneId
import com.github.kjarosh.agh.pp.rest.BasicQueriesController
import com.github.kjarosh.agh.pp.rest.client.ZoneClient
import io.ktor.application.Application
import io.ktor.application.call
import io.ktor.response.respond
import io.ktor.routing.post
import io.ktor.routing.routing

fun Application.gmmfNaiveRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    val graphLoader = GraphLoader()
    graphLoader.init()
    val basicQueriesController = BasicQueriesController(graphLoader)

    fun reaches(
        fromId: String,
        toId: String,
    ): Boolean {
        val graph: Graph = graphLoader.getGraph()
        val edgeId: EdgeId =
            EdgeId.of(
                VertexId(fromId),
                VertexId(toId),
            )
        val fromOwner: ZoneId = edgeId.getFrom().owner()

        if (fromOwner != ZONE_ID) {
            return ZoneClient().naive().reaches(fromOwner, edgeId).isReaches
        }

        if (basicQueriesController.isAdjacent(fromId, toId)) {
            return true
        }

        return graph.getEdgesBySource(edgeId.getFrom())
            .stream()
            .anyMatch { e ->
                try {
                    return@anyMatch reaches(e.dst().toString(), toId)
                } catch (err: StackOverflowError) {
                    throw RuntimeException("Found a cycle")
                }
            }
    }

    fun members(ofId: String): Collection<String> {
        val graph: Graph = graphLoader.getGraph()
        val of = VertexId(ofId)
        val ofOwner: ZoneId = of.owner()

        if (ofOwner != ZONE_ID) {
            return ZoneClient().naive().members(ofOwner, of).members
        }

        val result: MutableSet<String> = HashSet()

        for (edge in graph.getEdgesByDestination(of)) {
            result.add(edge.src().toString())
            try {
                result.addAll(members(edge.src().toString()))
            } catch (e: StackOverflowError) {
                throw RuntimeException("Found a cycle")
            }
        }

        return result
    }

    fun effectivePermissions(
        fromId: String,
        toId: String,
    ): String? {
        val graph: Graph = graphLoader.graph
        val edgeId: EdgeId =
            EdgeId.of(
                VertexId(fromId),
                VertexId(toId),
            )
        val fromOwner: ZoneId = edgeId.getFrom().owner()

        if (fromOwner != ZONE_ID) {
            return ZoneClient().naive().effectivePermissions(fromOwner, edgeId).effectivePermissions
        }

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
                    val other: String? = effectivePermissions(edge.dst().toString(), toId)
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
            call.respond(reaches(fromId, toId))
        }

        post("/gmmf/naive/members") {
            val ofId = call.parameters["of"]!!
            call.respond(members(ofId))
        }

        post("/gmmf/naive/effective_permissions") {
            val fromId = call.parameters["from"]!!
            val toId = call.parameters["to"]!!

            call.respond(effectivePermissions(fromId, toId) ?: "NONE")
        }
    }
}
