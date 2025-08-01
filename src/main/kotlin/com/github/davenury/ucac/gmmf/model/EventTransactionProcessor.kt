package com.github.davenury.ucac.gmmf.model

import com.github.davenury.common.Change
import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.PeersetId
import com.github.davenury.common.StandardChange
import com.github.davenury.ucac.common.PeersetProtocols
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.index.events.Event
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory

class EventTransactionProcessor(
    private val eventProcessor: EventProcessor,
    private val eventSender: EventSender,
    private val protocols: PeersetProtocols,
) {
    suspend fun process(
        vertexId: VertexId,
        event: Event,
    ): Boolean {
        logger.info("Processing event: ${event.id}")

        val currentEntryIdBefore = protocols.history.getCurrentEntryId()
        val result = eventProcessor.process(vertexId, event)
        val currentEntryIdAfter = protocols.history.getCurrentEntryId()

        if (currentEntryIdBefore != currentEntryIdAfter) {
            logger.info("Processing event ${event.id} failed, optimistic lock failure")
            return false
        }

        val tx = ProcessEventTx(vertexId, event.id, result.diff, result.generatedEvents)
        val change =
            StandardChange(
                tx.serialize(),
                peersets = listOf(ChangePeersetInfo(protocols.peersetId, currentEntryIdAfter)),
            )
        val changeResult = protocols.consensusProtocol.proposeChangeAsync(change).await()
        val success = changeResult.status == ChangeResult.Status.SUCCESS
        if (success) {
            logger.info("Successfully processed event ${event.id}")
        } else {
            logger.info("Failed to process event ${event.id}: $changeResult")
        }
        return success
    }

    suspend fun send(
        vertexId: VertexId,
        event: Event,
        postedEntryId: String,
    ): Boolean {
        if (!eventSender.send(vertexId, event)) {
            return false
        }

        val currentEntryId = protocols.history.getCurrentEntryId()
        val alreadySent =
            protocols.history.hasEntry(postedEntryId, currentEntryId) { entry ->
                val sentEventId =
                    Change.fromHistoryEntry(entry)?.getAppliedContent()?.let {
                        IndexTransaction.deserialize(it)?.getSentEventId()
                    }

                return@hasEntry sentEventId == event.id
            }

        if (alreadySent) {
            return false
        }

        val peersetId = PeersetId(vertexId.owner().id)
        val tx = SendOutboxEvent(peersetId, event.id)
        val change =
            StandardChange(
                tx.serialize(),
                peersets = listOf(ChangePeersetInfo(protocols.peersetId, currentEntryId)),
            )
        val changeResult = protocols.consensusProtocol.proposeChangeAsync(change).await()
        logger.info("Event sending transaction status: {}", changeResult)
        return changeResult.status == ChangeResult.Status.SUCCESS
    }

    companion object {
        private val logger = LoggerFactory.getLogger("evt-tx-proc")
    }
}
