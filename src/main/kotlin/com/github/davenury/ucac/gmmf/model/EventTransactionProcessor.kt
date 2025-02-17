package com.github.davenury.ucac.gmmf.model

import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.StandardChange
import com.github.davenury.ucac.common.PeersetProtocols
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.index.events.Event
import kotlinx.coroutines.future.await
import org.slf4j.LoggerFactory

class EventTransactionProcessor(private val eventProcessor: EventProcessor, private val protocols: PeersetProtocols) {
    suspend fun process(
        vertexId: VertexId,
        event: Event,
    ): Boolean {
        val result = eventProcessor.process(vertexId, event)

        val tx = ProcessEventTx(vertexId, event.id, result.diff, result.generatedEvents)
        val change =
            StandardChange(
                tx.serialize(),
                // TODO parent id?
                peersets = listOf(ChangePeersetInfo(protocols.peersetId, null)),
            )
        val changeResult = protocols.consensusProtocol.proposeChangeAsync(change).await()
        logger.info("Event processing transaction status: {}", changeResult)
        return changeResult.status == ChangeResult.Status.SUCCESS
    }

    companion object {
        private val logger = LoggerFactory.getLogger("evt-tx-proc")
    }
}
