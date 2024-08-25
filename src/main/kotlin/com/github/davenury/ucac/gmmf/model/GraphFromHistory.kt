package com.github.davenury.ucac.gmmf.model

import com.github.davenury.common.Change
import com.github.davenury.common.PeersetId
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.common.history.HistoryListener
import com.github.kjarosh.agh.pp.graph.model.Graph
import com.github.kjarosh.agh.pp.graph.model.ZoneId
import com.github.kjarosh.agh.pp.persistence.memory.InMemoryGraph

class GraphFromHistory(
    history: History,
    peersetId: PeersetId,
) {
    private val graph = InMemoryGraph(ZoneId(peersetId.peersetId))

    init {
        history.addListener(
            object : HistoryListener {
                override fun afterNewEntry(
                    entry: HistoryEntry,
                    successful: Boolean,
                ) {
                    this@GraphFromHistory.afterNewEntry(entry, successful)
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
            GraphTransaction.deserialize(it)?.apply(graph)
        }
    }

    fun getGraph(): Graph {
        return graph
    }
}
