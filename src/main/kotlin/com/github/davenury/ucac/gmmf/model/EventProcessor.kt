package com.github.davenury.ucac.gmmf.model

import com.github.kjarosh.agh.pp.graph.model.Graph
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.index.EffectiveVertex
import com.github.kjarosh.agh.pp.index.VertexIndices
import com.github.kjarosh.agh.pp.index.events.Event
import com.github.kjarosh.agh.pp.index.events.EventType
import java.util.UUID
import java.util.concurrent.ConcurrentSkipListSet
import java.util.function.Consumer

class EventProcessor(private val graph: Graph, private val vertexIndices: VertexIndices) {
    fun process(
        id: VertexId,
        event: Event,
    ): EventProcessingResult {
        val result = EventProcessingResult(id)
        processSingle(id, event, result)
        return result
    }

    private fun processSingle(
        id: VertexId,
        event: Event,
        result: EventProcessingResult,
    ) {
        return when (event.type) {
            EventType.CHILD_CHANGE -> {
                processChild(id, event, false, result)
            }

            EventType.PARENT_CHANGE -> {
                processParent(id, event, false, result)
            }

            EventType.CHILD_REMOVE -> {
                processChild(id, event, true, result)
            }

            EventType.PARENT_REMOVE -> {
                processParent(id, event, true, result)
            }

            else -> {
                throw AssertionError()
            }
        }
    }

    private fun processParent(
        id: VertexId,
        event: Event,
        delete: Boolean,
        result: EventProcessingResult,
    ) {
        val diff = result.diff
        val index = vertexIndices.getIndexOf(id)
        val effectiveParents: MutableSet<VertexId> = ConcurrentSkipListSet()
        for (subjectId in event.effectiveVertices) {
            if (delete) {
                index.getEffectiveParent(subjectId).ifPresent { effectiveVertex: EffectiveVertex ->
                    if (effectiveVertex.intermediateVertices.size.equals(setOf(event.sender))) {
                        diff.removeEffectiveParent(subjectId)
                        effectiveParents.add(subjectId)
                    } else {
                        val effectiveVertexDiff = diff.getOrAddEffectiveParentDiff(subjectId)
                        effectiveVertexDiff.removeIntermediateVertex(event.sender)
                    }
                }
            } else {
                val alreadyPresent =
                    index.getEffectiveParent(subjectId)
                        .map { it.intermediateVertices.contains(event.sender) }
                        .orElse(false)
                if (!alreadyPresent) {
                    val effectiveVertexDiff = diff.getOrAddEffectiveParentDiff(subjectId)
                    effectiveVertexDiff.addIntermediateVertex(event.sender)
                    effectiveParents.add(subjectId)
                }
            }
        }

        if (effectiveParents.isNotEmpty()) {
            val recipients = graph.getSourcesByDestination(id)
            propagateEvent(id, recipients, event, effectiveParents, event.type, result)
        }
    }

    private fun processChild(
        id: VertexId,
        event: Event,
        delete: Boolean,
        result: EventProcessingResult,
    ) {
        val diff = result.diff
        val index = vertexIndices.getIndexOf(id)

        val effectiveChildren: MutableSet<VertexId> = ConcurrentSkipListSet()
        for (subjectId in event.effectiveVertices) {
            if (delete) {
                index.getEffectiveChild(subjectId).ifPresent { effectiveVertex: EffectiveVertex ->
                    if (effectiveVertex.intermediateVertices.size.equals(setOf(event.sender))) {
                        diff.removeEffectiveChild(subjectId)
                        effectiveChildren.add(subjectId)
                    } else {
                        val effectiveVertexDiff = diff.getOrAddEffectiveChildDiff(subjectId)
                        effectiveVertexDiff.removeIntermediateVertex(event.sender)
                    }
                }
            } else {
                val alreadyPresent =
                    index.getEffectiveChild(subjectId)
                        .map { it.intermediateVertices.contains(event.sender) }
                        .orElse(false)
                if (!alreadyPresent) {
                    val effectiveVertexDiff = diff.getOrAddEffectiveChildDiff(subjectId)
                    effectiveVertexDiff.addIntermediateVertex(event.sender)
                    effectiveChildren.add(subjectId)
                }
            }
        }

        if (effectiveChildren.isNotEmpty()) {
            val recipients = graph.getDestinationsBySource(id)
            propagateEvent(id, recipients, event, effectiveChildren, event.type, result)
        }
    }

    private fun propagateEvent(
        sender: VertexId,
        recipients: Collection<VertexId>,
        event: Event,
        effectiveVertices: Set<VertexId>,
        type: EventType,
        result: EventProcessingResult,
    ) {
        recipients.forEach(
            Consumer { r: VertexId ->
                val newEvent =
                    Event.builder()
                        .id(UUID.randomUUID().toString())
                        .trace(event.trace)
                        .type(type)
                        .effectiveVertices(effectiveVertices)
                        .sender(sender)
                        .originalSender(event.originalSender)
                        .build()
                result.addNewGeneratedEvent(r, newEvent)
            },
        )
    }
}
