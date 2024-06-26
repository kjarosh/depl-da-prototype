package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.common.Change

data class Transaction(
    val ballotNumber: Int,
    val initVal: Accept,
    val acceptNum: Int = 0,
    val acceptVal: Accept? = null,
    val decision: Boolean = false,
    val ended: Boolean = false,
    val change: Change?,
)
