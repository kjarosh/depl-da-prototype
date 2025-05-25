package com.github.davenury.ucac.gmmf.model

import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.index.events.Event

data class PostedEvent(val event: Event, val vertexId: VertexId, val postedEntryId: String)
