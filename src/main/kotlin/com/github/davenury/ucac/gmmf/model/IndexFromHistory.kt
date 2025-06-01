package com.github.davenury.ucac.gmmf.model

import com.github.davenury.common.Change
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.common.history.HistoryListener
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.common.PeersetProtocols
import com.github.kjarosh.agh.pp.index.VertexIndices
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class IndexFromHistory(
    config: Config,
    history: History,
    private val graphFromHistory: GraphFromHistory,
    protocols: PeersetProtocols,
) {
    private val indices = VertexIndices()
    private val eventProcessor = EventProcessor(graphFromHistory.getGraph(), indices)
    private val eventSender = EventSender(protocols.peerResolver)
    private val eventTransactionProcessor = EventTransactionProcessor(eventProcessor, eventSender, protocols)
    val eventDatabase = EventDatabase(graphFromHistory.getGraph().currentZoneId, eventTransactionProcessor)

    init {
        if (config.indexing) {
            logger.info("Indexing enabled")
            initialize(history)
        } else {
            logger.info("Indexing disabled")
        }
    }

    private fun initialize(history: History) {
        history.addListener(
            object : HistoryListener {
                override fun afterNewEntry(
                    entry: HistoryEntry,
                    successful: Boolean,
                ) {
                    this@IndexFromHistory.afterNewEntry(entry, successful)
                }
            },
        )

        history.toEntryList().reversed().forEach { entry -> applyNewEntry(entry) }
    }

    private fun afterNewEntry(
        entry: HistoryEntry,
        successful: Boolean,
    ) {
        if (successful) {
            applyNewEntry(entry)
        }
    }

    private fun applyNewEntry(entry: HistoryEntry) {
        Change.fromHistoryEntry(entry)?.getAppliedContent()?.let {
            IndexTransaction.deserialize(it)?.apply(graphFromHistory.getGraph(), indices, eventDatabase, entry.getId())
            GraphTransaction.deserialize(it)?.applyEvents(graphFromHistory.getGraph(), indices, eventDatabase, entry.getId())
        }
    }

    fun getIndices(): VertexIndices {
        return indices
    }

    fun isReady(): Boolean {
        return eventDatabase.isEmpty()
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger("index")
    }
}
