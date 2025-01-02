package com.github.davenury.ucac.gmmf.model

import com.github.davenury.common.PeersetId
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.graph.model.ZoneId
import com.github.kjarosh.agh.pp.index.events.Event

class EventDatabase(private val currentZoneId: ZoneId) {
    val acceptedEventIds = HashSet<String>()
    val processedEventIds = HashSet<String>()
    private val outboxes: MutableMap<PeersetId, ArrayDeque<Event>> = HashMap()
    private val inboxes: MutableMap<VertexId, ArrayDeque<Event>> = HashMap()

    fun getInbox(id: VertexId): ArrayDeque<Event> {
        return inboxes.computeIfAbsent(id) { ArrayDeque() }
    }

    fun getOutbox(peersetId: PeersetId): ArrayDeque<Event> {
        return outboxes.computeIfAbsent(peersetId) { ArrayDeque() }
    }

    fun post(
        id: VertexId,
        event: Event,
    ) {
        if (id.owner() != currentZoneId) {
            getOutbox(PeersetId(id.owner().id)).addLast(event)
        } else {
            getInbox(id).addLast(event)
        }
    }
}
