package com.github.davenury.ucac.gmmf.routing

import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.Vertex
import com.github.kjarosh.agh.pp.graph.model.VertexId

data class NewVertex(val name: String, val type: Vertex.Type)

data class NewEdge(val from: VertexId, val to: VertexId, val permissions: Permissions)
