package com.github.davenury.ucac.gmmf.model

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.github.davenury.common.objectMapper
import com.github.kjarosh.agh.pp.graph.model.VertexId
import com.github.kjarosh.agh.pp.index.VertexIndices
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes(
    *arrayOf(
        JsonSubTypes.Type(value = ExampleTx::class, name = "example"),
    ),
)
sealed class IndexTransaction {
    fun serialize(): String {
        return objectMapper.writeValueAsString(this)
    }

    abstract fun apply(indices: VertexIndices)

    companion object {
        fun deserialize(content: String): IndexTransaction? {
            val logger: Logger = LoggerFactory.getLogger("index-tx")

            return objectMapper.readValue(content, IndexTransaction::class.java)
        }
    }
}

data class ExampleTx(val v: VertexId) : IndexTransaction() {
    override fun apply(indices: VertexIndices) {
        // TODO
    }
}
