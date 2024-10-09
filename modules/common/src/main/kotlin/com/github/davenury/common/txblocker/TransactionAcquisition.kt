package com.github.davenury.common.txblocker

import com.fasterxml.jackson.annotation.JsonProperty
import com.github.davenury.common.ProtocolName
import com.github.davenury.common.objectMapper

/**
 * @author Kamil Jarosz
 */
data class TransactionAcquisition(
    @JsonProperty("protocol")
    val protocol: ProtocolName,
    @JsonProperty("changeId")
    val changeId: String,
) {
    fun serialize(): String {
        return objectMapper.writeValueAsString(this)
    }

    override fun toString(): String {
        return "lock($protocol, $changeId)"
    }

    companion object {
        fun deserialize(value: String?): TransactionAcquisition? {
            return if (value != null && value != "") {
                objectMapper.readValue(value, TransactionAcquisition::class.java)
            } else {
                null
            }
        }
    }
}
