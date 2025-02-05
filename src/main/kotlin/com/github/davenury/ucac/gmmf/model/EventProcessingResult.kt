package com.github.davenury.ucac.gmmf.model

import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.index.events.Event

class EventProcessingResult(vertexId: VertexId) {
    var diff: IndexDiff = IndexDiff(vertexId)
    var generatedEvents: MutableMap<VertexId, ArrayDeque<Event>> = mutableMapOf()

    fun addNewGeneratedEvent(
        id: VertexId,
        event: Event,
    ) {
        generatedEvents.computeIfAbsent(id) { ArrayDeque() }.addLast(event)
    }
}
