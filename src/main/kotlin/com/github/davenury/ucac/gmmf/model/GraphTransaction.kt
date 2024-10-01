package com.github.davenury.ucac.gmmf.model

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.github.davenury.common.objectMapper
import com.github.kjarosh.agh.pp.graph.model.Edge
import com.github.kjarosh.agh.pp.graph.model.Graph
import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.Vertex
import com.github.kjarosh.agh.pp.graph.model.VertexId
import org.slf4j.Logger
import org.slf4j.LoggerFactory

val logger: Logger = LoggerFactory.getLogger("graph-tx")

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

    companion object {
        fun deserialize(content: String): GraphTransaction? {
            return objectMapper.readValue(content, GraphTransaction::class.java)
        }
    }
}

data class AddVertexTx(val id: VertexId, val type: Vertex.Type) : GraphTransaction() {
    override fun apply(graph: Graph) {
        graph.addVertex(Vertex(id, type))
        logger.info("Vertex {} added to the graph", id)
    }
}

data class AddEdgeTx(val from: VertexId, val to: VertexId, val permissions: Permissions) : GraphTransaction() {
    override fun apply(graph: Graph) {
        graph.addEdge(Edge(from, to, permissions))
        logger.info("Edge {}->{} added to the graph", from, to)
    }
}
