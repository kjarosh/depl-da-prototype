package com.github.davenury.ucac.gmmf.model

import com.github.davenury.ucac.gmmf.model.EffectiveVertexDiff.Companion.logger
import com.github.kjarosh.agh.pp.graph.model.Edge
import com.github.kjarosh.agh.pp.graph.model.Graph
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.index.EffectiveVertex
import com.github.kjarosh.agh.pp.index.VertexIndices
import org.slf4j.Logger
import org.slf4j.LoggerFactory

data class IndexDiff(
    val vertexId: VertexId,
    val effectiveChildren: HashMap<VertexId, EffectiveVertexDiff> = HashMap(),
    val effectiveParents: HashMap<VertexId, EffectiveVertexDiff> = HashMap(),
    val removeEffectiveChildren: HashSet<VertexId> = HashSet(),
    val removeEffectiveParents: HashSet<VertexId> = HashSet(),
) {
    fun apply(
        graph: Graph,
        indices: VertexIndices,
    ) {
        val index = indices.getIndexOf(vertexId)

        removeEffectiveChildren.forEach {
            logger.info("Removing effective child $it")
            index.removeEffectiveChild(it)
        }

        removeEffectiveParents.forEach {
            logger.info("Removing effective parent $it")
            index.removeEffectiveParent(it)
        }

        val edgesToCalculate = graph.getEdgesByDestination(vertexId)

        effectiveChildren.forEach {
            it.value.apply(it.key, index.getOrAddEffectiveChild(it.key), edgesToCalculate)
        }

        effectiveParents.forEach {
            it.value.apply(it.key, index.getOrAddEffectiveParent(it.key), edgesToCalculate)
        }
    }

    fun getOrAddEffectiveChildDiff(subjectId: VertexId): EffectiveVertexDiff {
        return effectiveChildren.computeIfAbsent(subjectId) { EffectiveVertexDiff() }
    }

    fun removeEffectiveChild(subjectId: VertexId) {
        removeEffectiveChildren.add(subjectId)
    }

    fun getOrAddEffectiveParentDiff(subjectId: VertexId): EffectiveVertexDiff {
        return effectiveParents.computeIfAbsent(subjectId) { EffectiveVertexDiff() }
    }

    fun removeEffectiveParent(subjectId: VertexId) {
        removeEffectiveParents.add(subjectId)
    }
}

data class EffectiveVertexDiff(
    val newIntermediateVertices: HashSet<VertexId> = HashSet(),
    val removedIntermediateVertices: HashSet<VertexId> = HashSet(),
) {
    fun addIntermediateVertex(vertex: VertexId) {
        newIntermediateVertices.add(vertex)
    }

    fun removeIntermediateVertex(vertex: VertexId) {
        removedIntermediateVertices.add(vertex)
    }

    fun apply(
        vertex: VertexId,
        effectiveVertex: EffectiveVertex,
        edgesToCalculate: MutableSet<Edge>,
    ) {
        if (newIntermediateVertices.isNotEmpty()) {
            logger.info("Adding intermediate vertices to $vertex: $newIntermediateVertices")
        }
        if (removedIntermediateVertices.isNotEmpty()) {
            logger.info("Removing intermediate vertices to $vertex: $removedIntermediateVertices")
        }
        effectiveVertex.addIntermediateVertices(newIntermediateVertices) {}
        effectiveVertex.removeIntermediateVertices(removedIntermediateVertices) {}
        effectiveVertex.recalculatePermissions(edgesToCalculate)
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger("diff")
    }
}
