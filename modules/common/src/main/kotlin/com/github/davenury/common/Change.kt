package com.github.davenury.common

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.core.JsonProcessingException
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.common.history.IntermediateHistoryEntry
import java.util.Objects
import java.util.UUID

// see https://github.com/FasterXML/jackson-databind/issues/2742#issuecomment-637708397
class Changes : ArrayList<Change> {
    constructor() : super()

    constructor(collection: List<Change>) : super(collection)

    companion object {
        fun fromHistory(history: History): Changes {
            return history.toEntryList()
                .reversed()
                .mapNotNull { Change.fromHistoryEntry(it) }
                .let { Changes(it) }
        }
    }
}

data class ChangePeersetInfo(
    val peersetId: PeersetId,
    val parentId: String?,
) {
    override fun toString(): String {
        return "$peersetId=$parentId"
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes(
    *arrayOf(
        JsonSubTypes.Type(value = StandardChange::class, name = "standard"),
        JsonSubTypes.Type(value = TwoPCChange::class, name = "2pc"),
    ),
)
sealed class Change(open val id: String = UUID.randomUUID().toString()) {
    abstract val peersets: List<ChangePeersetInfo>
    abstract val notificationUrl: String?

    fun getPeersetInfo(peersetId: PeersetId): ChangePeersetInfo? = peersets.find { it.peersetId == peersetId }

    fun toHistoryEntry(
        peersetId: PeersetId,
        parentIdDefault: String? = null,
    ): HistoryEntry {
        val info =
            getPeersetInfo(peersetId)
                ?: throw IllegalArgumentException("Unknown peersetId: $peersetId")
        return IntermediateHistoryEntry(
            objectMapper.writeValueAsString(this),
            info.parentId
                ?: parentIdDefault
                ?: throw IllegalArgumentException("No parent ID"),
        )
    }

    abstract fun copyWithNewParentId(
        peersetId: PeersetId,
        parentId: String?,
    ): Change

    abstract fun getAppliedContent(): String?

    protected fun doesEqual(other: Any?): Boolean = (other is Change) && Objects.equals(id, other.id)

    companion object {
        private fun fromJson(json: String): Change = objectMapper.readValue(json, Change::class.java)

        fun fromHistoryEntry(entry: HistoryEntry): Change? {
            if (entry == InitialHistoryEntry) {
                return null
            }

            return try {
                fromJson(entry.getContent())
            } catch (e: JsonProcessingException) {
                null
            }
        }
    }
}

enum class TwoPCStatus {
    ACCEPTED,
    ABORTED,
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class StandardChange(
    val content: String,
    @JsonProperty("notification_url")
    override val notificationUrl: String? = null,
    override val id: String = UUID.randomUUID().toString(),
    override val peersets: List<ChangePeersetInfo>,
) : Change(id) {
    override fun equals(other: Any?): Boolean {
        if (other !is StandardChange || !super.doesEqual(other)) {
            return false
        }
        return Objects.equals(peersets, other.peersets) && Objects.equals(content, other.content)
    }

    override fun hashCode(): Int {
        return Objects.hash(peersets, content)
    }

    override fun copyWithNewParentId(
        peersetId: PeersetId,
        parentId: String?,
    ): Change =
        this.copy(
            peersets =
                peersets.map {
                    if (it.peersetId == peersetId) {
                        ChangePeersetInfo(peersetId, parentId)
                    } else {
                        it
                    }
                },
        )

    override fun getAppliedContent(): String {
        return content
    }

    override fun toString(): String {
        return "StandardChange($id, $peersets, $content)"
    }
}

// TwoPC should always contain two changes:
// If accepted: 2PCChange-Accept -> Change
// Else: 2PCChange-Accept -> 2PCChange-Abort
@JsonIgnoreProperties(ignoreUnknown = true)
data class TwoPCChange(
    @JsonProperty("notification_url")
    override val notificationUrl: String? = null,
    val twoPCStatus: TwoPCStatus,
    val change: Change,
    override val id: String = UUID.randomUUID().toString(),
    override val peersets: List<ChangePeersetInfo>,
    val leaderPeerset: PeersetId,
) : Change(id) {
    override fun equals(other: Any?): Boolean {
        if (other !is TwoPCChange || !super.doesEqual(other)) {
            return false
        }
        return Objects.equals(peersets, other.peersets) && Objects.equals(twoPCStatus, other.twoPCStatus) &&
            Objects.equals(this.change, other.change)
    }

    override fun hashCode(): Int {
        return Objects.hash(peersets, twoPCStatus)
    }

    override fun copyWithNewParentId(
        peersetId: PeersetId,
        parentId: String?,
    ): Change =
        this.copy(
            peersets =
                peersets.map {
                    if (it.peersetId == peersetId) {
                        ChangePeersetInfo(peersetId, parentId)
                    } else {
                        it
                    }
                },
        )

    override fun getAppliedContent(): String? {
        return null
    }

    override fun toString(): String {
        return "2PCChange($id, $twoPCStatus, $peersets (leader $leaderPeerset), $change)"
    }
}
