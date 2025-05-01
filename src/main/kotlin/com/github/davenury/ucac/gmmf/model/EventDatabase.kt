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
    private val outboxes: MutableMap<PeersetId, ArrayDeque<Pair<VertexId, Event>>> = ConcurrentHashMap()
    private val inboxes: MutableMap<VertexId, ArrayDeque<Event>> = ConcurrentHashMap()

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

    fun getInbox(id: VertexId): ArrayDeque<Event> {
        return inboxes.computeIfAbsent(id) { ArrayDeque() }
    }

    fun getOutbox(peersetId: PeersetId): ArrayDeque<Pair<VertexId, Event>> {
        return outboxes.computeIfAbsent(peersetId) { ArrayDeque() }
    }

    fun post(
        id: VertexId,
        event: Event,
    ) {
        if (id.owner() != currentZoneId) {
            val peersetId = PeersetId(id.owner().id)
            logger.info("Posting an event: ${event.id} to outbox $peersetId")
            getOutbox(peersetId).addLast(Pair(id, event))
        } else {
            logger.info("Posting an event: ${event.id} to inbox $id")
            getInbox(id).addLast(event)
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
                val event = queue.first()
                processed = processed || eventTransactionProcessor.process(vertexId, event)
            }
        }

        for (e in outboxes) {
            val queue = e.value

            if (queue.isNotEmpty()) {
                // Do not remove the event here, we remove events on tx commit.
                val pair = queue.first()
                val vertexId = pair.first
                val event = pair.second
                processed = processed || eventTransactionProcessor.send(vertexId, event)
            }
        }

        return processed
    }

    companion object {
        private val logger = LoggerFactory.getLogger("evt-db")
    }
}
