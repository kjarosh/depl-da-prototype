package com.github.davenury.ucac.consensus.ratis

import com.fasterxml.jackson.databind.JsonMappingException
import com.github.davenury.ucac.common.Change
import com.github.davenury.ucac.common.ChangeWithAcceptNum
import com.github.davenury.ucac.common.History
import com.github.davenury.ucac.objectMapper
import org.slf4j.LoggerFactory


class HistoryStateMachine(override var state: History = mutableListOf()) :
    StateMachine<History>(state) {

    override fun serializeState(): String = objectMapper.writeValueAsString(state)

    override fun applyOperation(operation: String): String? =
        try {
            state.add(ChangeWithAcceptNum.fromJson(operation))
            null
        } catch (e: JsonMappingException) {
            logger.error("Error during applyOperation ${e.message}")
            e.message
        }

    override fun queryOperation(operation: String): String =
        try {
            OperationType.valueOf(operation).invokeOperation(state)
        } catch (e: java.lang.IllegalArgumentException) {
            logger.error("Error during queryOperation: illegalArgumentException")
            "INVALID_OPERATION"
        }

    enum class OperationType {
        LAST {
            override fun invokeOperation(state: History): String {
                return state.last().let { objectMapper.writeValueAsString(it) }
            }
        },
        STATE {
            override fun invokeOperation(state: History): String {
                return objectMapper.writeValueAsString(state)
            }
        };

        abstract fun invokeOperation(state: History): String
    }

    companion object {
        private val logger = LoggerFactory.getLogger(HistoryStateMachine::class.java)
    }
}
