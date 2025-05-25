package com.github.davenury.common.history

import java.util.function.Predicate

/**
 * @author Kamil Jarosz
 */
interface History {
    fun getCurrentEntryId(): String

    fun getCurrentEntry(): HistoryEntry

    fun addEntry(entry: HistoryEntry)

    fun addListener(listener: HistoryListener)

    @Throws(EntryNotFoundException::class)
    fun getEntry(id: String): HistoryEntry

    fun getEntryFromHistory(id: String): HistoryEntry? {
        var entry = getCurrentEntry()
        while (true) {
            if (entry.getId() == id) {
                return entry
            }

            if (entry.getParentId() == null) {
                return null
            } else {
                entry = getEntry(entry.getParentId()!!)
            }
        }
    }

    fun toEntryList(skipInitial: Boolean = true): List<HistoryEntry> {
        val list = ArrayList<HistoryEntry>()
        var entry = getCurrentEntry()
        while (true) {
            if (skipInitial && entry == InitialHistoryEntry) {
                return list
            }
            list.add(entry)
            val parentId = entry.getParentId() ?: return list
            entry = getEntryFromHistory(parentId)!!
        }
    }

    fun containsEntry(entryId: String): Boolean {
        return getEntryFromHistory(entryId) != null
    }

    fun isEntryCompatible(entry: HistoryEntry): Boolean {
        return containsEntry(entry.getId()) || getCurrentEntryId() == entry.getParentId()
    }

    fun getAllEntriesUntilHistoryEntryId(historyEntryId: String): List<HistoryEntry> =
        if (containsEntry(historyEntryId)) {
            val resultList: MutableList<HistoryEntry> = mutableListOf()
            var entry = getCurrentEntry()
            while (entry.getId() != historyEntryId) {
                resultList.add(entry)
                entry = getEntry(entry.getParentId()!!)
            }

            resultList.reversed()
        } else {
            listOf()
        }

    fun hasEntry(
        fromEntryId: String? = null,
        toEntryId: String? = null,
        predicate: Predicate<HistoryEntry>,
    ): Boolean {
        // Go in the reverse direction, following parents
        var entryId = toEntryId ?: getCurrentEntryId()
        while (entryId != fromEntryId) {
            val entry = getEntry(entryId)
            if (predicate.test(entry)) {
                return true
            }

            entryId = entry.getParentId() ?: break
        }

        return false
    }
}
