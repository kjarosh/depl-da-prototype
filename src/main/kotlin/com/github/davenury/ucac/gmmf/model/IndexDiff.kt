package com.github.davenury.ucac.gmmf.model

import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.index.EffectiveVertex
import com.github.kjarosh.agh.pp.index.VertexIndices

class IndexDiff(val vertexId: VertexId) {
    val newEffectiveChildren: Map<VertexId, EffectiveVertex> = HashMap()
    val removedEffectiveChildren: Set<VertexId> = HashSet()

    val newEffectiveParents: Map<VertexId, EffectiveVertex> = HashMap()
    val removedEffectiveParents: Set<VertexId> = HashSet()

    fun apply(indices: VertexIndices) {
        val index = indices.getIndexOf(vertexId)

        newEffectiveChildren.forEach {
            val effectiveVertex = index.getOrAddEffectiveChild(it.key)
        }
    }
}
