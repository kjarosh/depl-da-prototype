package com.github.davenury.ucac.gmmf.model

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.core.JacksonException
import com.github.davenury.common.PeersetId
import com.github.davenury.common.objectMapper
import com.github.kjarosh.agh.pp.graph.model.Graph
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.index.VertexIndices
import com.github.kjarosh.agh.pp.index.events.Event
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes(
    *arrayOf(
        JsonSubTypes.Type(value = AcceptExternalEvent::class, name = "accept"),
        JsonSubTypes.Type(value = ProcessEventTx::class, name = "process"),
        JsonSubTypes.Type(value = SendOutboxEvent::class, name = "send"),
    ),
)
sealed class IndexTransaction {
    fun serialize(): String {
        return objectMapper.writeValueAsString(this)
    }

    abstract fun apply(
        graph: Graph,
        indices: VertexIndices,
        eventDatabase: EventDatabase,
    )

    companion object {
        val logger: Logger = LoggerFactory.getLogger("index-tx")

        fun deserialize(content: String): IndexTransaction? {
            return try {
                objectMapper.readValue(content, IndexTransaction::class.java)
            } catch (e: JacksonException) {
                null
            }
        }
    }
}

data class AcceptExternalEvent(val vertex: VertexId, val event: Event) : IndexTransaction() {
    override fun apply(
        graph: Graph,
        indices: VertexIndices,
        eventDatabase: EventDatabase,
    ) {
        logger.info("Applying accepting external event {}", event.id)
        eventDatabase.post(vertex, event)
        eventDatabase.acceptedEventIds.add(event.id)
    }
}

data class ProcessEventTx(val vertex: VertexId, val eventId: String, val diff: IndexDiff, val generatedEvents: Map<VertexId, List<Event>>) : IndexTransaction() {
    override fun apply(
        graph: Graph,
        indices: VertexIndices,
        eventDatabase: EventDatabase,
    ) {
        logger.info("Applying processed event {}", eventId)
        val existingId = eventDatabase.getInbox(vertex).removeFirst().id
        if (existingId != eventId) {
            logger.error("ProcessEventTx: Event ID mismatch, expected $eventId, was $existingId")
            throw RuntimeException("ProcessEventTx: Event ID mismatch, expected $eventId, was $existingId")
        }

        diff.apply(graph, indices)

        eventDatabase.processedEventIds.add(eventId)

        generatedEvents.forEach {
            it.value.forEach { event ->
                eventDatabase.post(it.key, event)
            }
        }
    }
}

data class SendOutboxEvent(val peersetId: PeersetId, val eventId: String) : IndexTransaction() {
    override fun apply(
        graph: Graph,
        indices: VertexIndices,
        eventDatabase: EventDatabase,
    ) {
        logger.info("Applying sent event {}", eventId)
        val existingId = eventDatabase.getOutbox(peersetId).removeFirst().second.id
        if (existingId != eventId) {
            logger.error("SendOutboxEvent: Event ID mismatch, expected $eventId, was $existingId")
            throw RuntimeException("SendOutboxEvent: Event ID mismatch, expected $eventId, was $existingId")
        }
    }
}
