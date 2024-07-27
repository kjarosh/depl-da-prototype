package com.github.davenury.ucac.gmmf.model

import com.github.davenury.common.objectMapper
import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.Vertex
import com.github.kjarosh.agh.pp.graph.model.VertexId

sealed class GraphTransaction {
    fun serialize(): String {
        return objectMapper.writeValueAsString(this)
    }
}

data class AddVertexTx(val name: String, val type: Vertex.Type) : GraphTransaction()

data class AddLocalEdgeTx(val fromName: String, val toName: String, val permissions: Permissions) : GraphTransaction()

data class AddExternalEdgeTx(val from: VertexId, val to: VertexId, val permissions: Permissions) : GraphTransaction()
