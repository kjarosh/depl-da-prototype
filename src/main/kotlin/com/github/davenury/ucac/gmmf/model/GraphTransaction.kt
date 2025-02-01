package com.github.davenury.ucac.gmmf.model

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.core.JacksonException
import com.github.davenury.common.objectMapper
import com.github.kjarosh.agh.pp.graph.model.Edge
import com.github.kjarosh.agh.pp.graph.model.EdgeId
import com.github.kjarosh.agh.pp.graph.model.Graph
import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.Vertex
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.index.VertexIndices
import com.github.kjarosh.agh.pp.index.events.Event
import com.github.kjarosh.agh.pp.index.events.EventType
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes(
    *arrayOf(
        JsonSubTypes.Type(value = AddVertexTx::class, name = "add_vertex"),
        JsonSubTypes.Type(value = AddEdgeTx::class, name = "add_edge"),
    ),
)
sealed class GraphTransaction {
    fun serialize(): String {
        return objectMapper.writeValueAsString(this)
    }

    abstract fun apply(graph: Graph)

    abstract fun applyEvents(
        graph: Graph,
        indices: VertexIndices,
        eventDatabase: EventDatabase,
    )

    companion object {
        val logger: Logger = LoggerFactory.getLogger("graph-tx")

        fun deserialize(content: String): GraphTransaction? {
            try {
                return objectMapper.readValue(content, GraphTransaction::class.java)
            } catch (e: JacksonException) {
                return null
            }
        }
    }
}

data class AddVertexTx(val id: VertexId, val type: Vertex.Type) : GraphTransaction() {
    override fun apply(graph: Graph) {
        graph.addVertex(Vertex(id, type))
        logger.info("Vertex {} added to the graph", id)
    }

    override fun applyEvents(
        graph: Graph,
        indices: VertexIndices,
        eventDatabase: EventDatabase,
    ) {
        // adding a vertex does not generate any events
    }
}

data class AddEdgeTx(
    val from: VertexId,
    val to: VertexId,
    val permissions: Permissions,
    val eventId: String,
    val reverseEventId: String,
) : GraphTransaction() {
    override fun apply(graph: Graph) {
        graph.addEdge(Edge(from, to, permissions))
        logger.info("Edge {}->{} added to the graph", from, to)
    }

    override fun applyEvents(
        graph: Graph,
        indices: VertexIndices,
        eventDatabase: EventDatabase,
    ) {
        if (from.owner() == graph.currentZoneId) {
            postChangeEvent(graph, indices, eventDatabase, false, eventId, EdgeId(from, to), false)
        }
        if (to.owner() == graph.currentZoneId) {
            postChangeEvent(graph, indices, eventDatabase, true, reverseEventId, EdgeId(from, to), false)
        }
    }
}

private fun postChangeEvent(
    graph: Graph,
    indices: VertexIndices,
    eventDatabase: EventDatabase,
    reverseDirection: Boolean,
    eventId: String,
    edgeId: EdgeId,
    delete: Boolean,
) {
    val subjects: MutableSet<VertexId> = HashSet()
    if (reverseDirection) {
        subjects.addAll(
            indices.getIndexOf(graph.getVertex(edgeId.to))
                .getEffectiveParentsSet(),
        )
        subjects.add(edgeId.to)

        eventDatabase.post(
            edgeId.from,
            Event.builder()
                .id(eventId)
                .type(if (delete) EventType.PARENT_REMOVE else EventType.PARENT_CHANGE)
                .effectiveVertices(subjects)
                .sender(edgeId.to)
                .originalSender(edgeId.to)
                .build(),
        )
    } else {
        subjects.addAll(
            indices.getIndexOf(graph.getVertex(edgeId.from))
                .getEffectiveChildrenSet(),
        )
        subjects.add(edgeId.from)
        eventDatabase.post(
            edgeId.to,
            Event.builder()
                .id(eventId)
                .type(if (delete) EventType.CHILD_REMOVE else EventType.CHILD_CHANGE)
                .effectiveVertices(subjects)
                .sender(edgeId.from)
                .originalSender(edgeId.from)
                .build(),
        )
    }
}
