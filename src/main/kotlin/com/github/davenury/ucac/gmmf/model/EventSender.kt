package com.github.davenury.ucac.gmmf.model

import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.gmmf.client.GmmfClient
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.index.events.Event

class EventSender(private val peerResolver: PeerResolver) {
    suspend fun send(
        vertexId: VertexId,
        event: Event,
    ): Boolean {
        val address = peerResolver.getPeerFromPeerset(PeersetId(vertexId.owner().id))
        return GmmfClient(peerResolver, address).sendEvent(vertexId, event)
    }
}
