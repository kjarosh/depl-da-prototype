package com.github.davenury.ucac.gmmf.model

import com.github.davenury.common.Change
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.common.history.HistoryListener
import com.github.kjarosh.agh.pp.index.VertexIndices

class IndexFromHistory(
    history: History,
    private val graphFromHistory: GraphFromHistory,
) {
    private val indices = VertexIndices()
    private val eventDatabase = EventDatabase(graphFromHistory.getGraph().currentZoneId)

    init {
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
            IndexTransaction.deserialize(it)?.apply(indices, eventDatabase)
            GraphTransaction.deserialize(it)?.applyEvents(graphFromHistory.getGraph(), indices, eventDatabase)
        }
    }

    fun getIndices(): VertexIndices {
        return indices
    }
}
