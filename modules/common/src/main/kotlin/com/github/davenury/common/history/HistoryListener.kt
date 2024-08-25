package com.github.davenury.common.history

interface HistoryListener {
    fun beforeNewEntry(entry: HistoryEntry) {}

    fun afterNewEntry(
        entry: HistoryEntry,
        successful: Boolean,
    ) {}
}
