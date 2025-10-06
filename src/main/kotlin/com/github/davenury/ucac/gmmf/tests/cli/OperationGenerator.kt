package com.github.davenury.ucac.gmmf.tests.cli

import com.github.kjarosh.agh.pp.graph.model.Edge
import com.github.kjarosh.agh.pp.graph.model.EdgeId
import com.github.kjarosh.agh.pp.graph.model.Graph
import java.util.Random
import java.util.concurrent.ConcurrentMap
import java.util.stream.Collectors

class OperationGenerator(graph: Graph) {
    class Operation(
        val delete: Boolean,
        val edge: Edge,
    )

    val random = Random(1)

    val edges: List<Edge> =
        graph.allEdges()
            .stream()
            .collect(Collectors.toList())

    val edgesDeleted: ConcurrentMap<EdgeId, Boolean> =
        edges
            .stream()
            .collect(Collectors.toConcurrentMap({ it.id() }, { false }))

    fun nextOperation(): Operation {
        val nextEdge =
            edges.get(random.nextInt(edges.size))
        val delete = edgesDeleted.computeIfPresent(nextEdge.id(), { _, v -> !v })!!

        return Operation(delete, nextEdge)
    }
}
