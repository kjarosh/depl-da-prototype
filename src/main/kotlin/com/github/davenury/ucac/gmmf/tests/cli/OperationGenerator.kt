package com.github.davenury.ucac.gmmf.tests.cli

import com.github.kjarosh.agh.pp.graph.model.EdgeId
import com.github.kjarosh.agh.pp.graph.model.Graph
import java.util.Random
import java.util.concurrent.ConcurrentMap
import java.util.stream.Collectors

class OperationGenerator(graph: Graph) {
    val random = Random(1)

    val edges: List<EdgeId> =
        graph.allEdges()
            .stream()
            .map { it.id() }
            .collect(Collectors.toList())

    val edgesDeleted: ConcurrentMap<EdgeId, Boolean> =
        edges
            .stream()
            .collect(Collectors.toConcurrentMap({ it }, { false }))

    fun nextOperation() {
        val nextEdge =
            edges.get(random.nextInt(0, edges.size))
        val delete = edgesDeleted.computeIfPresent(nextEdge, { _, v -> !v })!!

        println(delete)
        println(nextEdge)
    }
}
