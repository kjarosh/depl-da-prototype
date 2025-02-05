package com.github.kjarosh.agh.pp.index

import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.persistence.Persistence
import java.util.HashMap

/**
 * @author Kamil Jarosz
 */
class VertexIndices {
    private val indices: MutableMap<VertexId, VertexIndex> = HashMap()

    fun getIndexOf(id: VertexId): VertexIndex {
        return indices.computeIfAbsent(id) { i: VertexId? ->
            Persistence.getPersistenceFactory()
                .createIndex(id.toString())
        }
    }
}
