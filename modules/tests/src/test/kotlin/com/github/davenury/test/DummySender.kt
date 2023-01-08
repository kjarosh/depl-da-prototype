package com.github.davenury.test

import com.github.davenury.common.*
import com.github.davenury.tests.ChangeState
import com.github.davenury.tests.Changes
import com.github.davenury.tests.Sender
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.Phaser

class DummySender(
    private val peers: Map<Int, List<String>>,
    private val shouldNotify: Boolean,
    private val phaser: Phaser? = null
) : Sender {

    private lateinit var changes: Changes

    val appearedChanges = mutableListOf<Pair<String, Change>>()
    val mutex = Mutex()

    override suspend fun executeChange(address: String, change: Change): ChangeState {
        mutex.withLock {
            appearedChanges.add(Pair(address, change))
        }
        if (shouldNotify) {
            notify(change)
        }
        phaser?.arrive()
        return ChangeState.ACCEPTED
    }

    suspend fun notify(change: Change) {
        changes.handleNotification(
            Notification(
                change = change,
                result = ChangeResult(ChangeResult.Status.SUCCESS)
            )
        )
    }

    fun setChanges(changes: Changes) {
        this.changes = changes
    }

    fun getLastChange(): Change = appearedChanges.last().second
}