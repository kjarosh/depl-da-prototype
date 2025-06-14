package com.github.davenury.ucac.gmmf.model

import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.utils.MdcProvider
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.graph.model.ZoneId
import com.github.kjarosh.agh.pp.index.events.Event
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.slf4j.MDCContext
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors

class EventDatabase(private val currentZoneId: ZoneId, private val eventTransactionProcessor: EventTransactionProcessor) {
    private val mdcProvider = MdcProvider(mapOf("peerset" to currentZoneId.toString()))

    val acceptedEventIds = HashSet<String>()
    val processedEventIds = HashSet<String>()
    private val outboxes: MutableMap<PeersetId, ArrayDeque<PostedEvent>> = ConcurrentHashMap()
    private val inboxes: MutableMap<VertexId, ArrayDeque<PostedEvent>> = ConcurrentHashMap()

    private val executorService: ExecutorCoroutineDispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

    init {
        mdcProvider.withMdc {
            with(CoroutineScope(executorService)) {
                launch(MDCContext()) {
                    processEvents()
                }
            }
        }
    }

    fun getInbox(id: VertexId): ArrayDeque<PostedEvent> {
        return inboxes.computeIfAbsent(id) { ArrayDeque() }
    }

    fun getOutbox(peersetId: PeersetId): ArrayDeque<PostedEvent> {
        return outboxes.computeIfAbsent(peersetId) { ArrayDeque() }
    }

    fun post(
        id: VertexId,
        event: Event,
        postedEntryId: String,
    ) {
        val postedEvent = PostedEvent(event, id, postedEntryId)
        if (id.owner() != currentZoneId) {
            val peersetId = PeersetId(id.owner().id)
            logger.info("Posting an event: ${event.id} to outbox $peersetId")
            getOutbox(peersetId).addLast(postedEvent)
        } else {
            logger.info("Posting an event: ${event.id} to inbox $id")
            getInbox(id).addLast(postedEvent)
        }
    }

    fun isEmpty(): Boolean {
        return outboxes.values.all { it.isEmpty() } && inboxes.values.all { it.isEmpty() }
    }

    private suspend fun processEvents() {
        while (true) {
            val processed = processEvent()
            if (!processed) {
                // TODO better control?
                delay(100)
            }
        }
    }

    private suspend fun processEvent(): Boolean {
        var processed = false
        for (e in inboxes) {
            val vertexId = e.key
            val queue = e.value

            if (queue.isNotEmpty()) {
                // Do not remove the event here, we remove events on tx commit.
                val postedEvent = queue.first()
                val event = postedEvent.event
                processed = processed || eventTransactionProcessor.process(vertexId, event)
            }
        }

        for (e in outboxes) {
            val queue = e.value

            if (queue.isNotEmpty()) {
                // Do not remove the event here, we remove events on tx commit.
                val postedEvent = queue.first()
                val vertexId = postedEvent.vertexId
                val event = postedEvent.event
                val postedEntryId = postedEvent.postedEntryId
                processed = processed || eventTransactionProcessor.send(vertexId, event, postedEntryId)
            }
        }

        return processed
    }

    companion object {
        private val logger = LoggerFactory.getLogger("evt-db")
    }
}
