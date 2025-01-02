package com.github.davenury.ucac.gmmf.model

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.databind.DatabindException
import com.github.davenury.common.PeersetId
import com.github.davenury.common.objectMapper
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.index.VertexIndices
import com.github.kjarosh.agh.pp.index.events.Event
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes(
    *arrayOf(
        JsonSubTypes.Type(value = AcceptExternalEvent::class, name = "accept"),
        JsonSubTypes.Type(value = ProcessEvent::class, name = "process"),
        JsonSubTypes.Type(value = SendOutboxEvent::class, name = "send"),
    ),
)
sealed class IndexTransaction {
    fun serialize(): String {
        return objectMapper.writeValueAsString(this)
    }

    abstract fun apply(
        indices: VertexIndices,
        eventDatabase: EventDatabase,
    )

    companion object {
        val logger: Logger = LoggerFactory.getLogger("index-tx")

        fun deserialize(content: String): IndexTransaction? {
            return try {
                objectMapper.readValue(content, IndexTransaction::class.java)
            } catch (e: DatabindException) {
                null
            }
        }
    }
}

data class AcceptExternalEvent(val vertex: VertexId, val event: Event) : IndexTransaction() {
    override fun apply(
        indices: VertexIndices,
        eventDatabase: EventDatabase,
    ) {
        eventDatabase.getInbox(vertex).addLast(event)
        eventDatabase.acceptedEventIds.add(event.id)
    }
}

data class ProcessEvent(val vertex: VertexId, val eventId: String, val diff: IndexDiff, val generatedEvents: Map<PeersetId, List<Event>>) : IndexTransaction() {
    override fun apply(
        indices: VertexIndices,
        eventDatabase: EventDatabase,
    ) {
        val existingId = eventDatabase.getInbox(vertex).removeFirst().id
        if (existingId != eventId) {
            logger.error("Event ID mismatch, expected $eventId, was $existingId")
            // TODO better handling?
            exitProcess(1)
        }

        diff.apply(indices)

        eventDatabase.processedEventIds.add(eventId)

        generatedEvents.forEach {
            it.value.forEach { event ->
                eventDatabase.getOutbox(it.key).addLast(event)
            }
        }
    }
}

data class SendOutboxEvent(val peersetId: PeersetId, val eventId: String) : IndexTransaction() {
    override fun apply(
        indices: VertexIndices,
        eventDatabase: EventDatabase,
    ) {
        val existingId = eventDatabase.getOutbox(peersetId).removeFirst().id
        if (existingId != eventId) {
            logger.error("Event ID mismatch, expected $eventId, was $existingId")
            // TODO better handling?
            exitProcess(1)
        }
    }
}
