package com.github.davenury.ucac.gmmf.routing

import com.github.kjarosh.agh.pp.graph.model.Permissions
import com.github.kjarosh.agh.pp.graph.model.Vertex
import com.github.kjarosh.agh.pp.graph.model.VertexId

data class VertexMessage(val name: String, val type: Vertex.Type)

data class EdgeMessage(val from: VertexId, val to: VertexId, val permissions: Permissions)

data class ReachesMessage(val reaches: Boolean)

data class MembersMessage(val members: Set<VertexId>)

data class EffectivePermissionsMessage(val effectivePermissions: Permissions?)
