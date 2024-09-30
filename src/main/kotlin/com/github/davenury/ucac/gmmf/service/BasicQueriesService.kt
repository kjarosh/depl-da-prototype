package com.github.davenury.ucac.gmmf.service

import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.common.PeersetProtocols
import com.github.kjarosh.agh.pp.graph.model.EdgeId
import com.github.kjarosh.agh.pp.graph.model.Graph
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.graph.model.ZoneId

class BasicQueriesService(private val peersetProtocols: PeersetProtocols) {
    private fun graph(): Graph {
        return peersetProtocols.graphFromHistory.getGraph()
    }

    private fun peersetId(): PeersetId {
        return peersetProtocols.peersetId
    }

    fun isAdjacent(
        fromId: VertexId,
        toId: VertexId,
    ): Boolean {
        val edgeId = EdgeId.of(fromId, toId)
        val fromOwner = edgeId.from.owner()

        if (fromOwner != ZoneId(peersetId().peersetId)) {
            // TODO Forward request to fromOwner
            TODO()
        }

        return graph().hasEdge(edgeId)
    }
}
