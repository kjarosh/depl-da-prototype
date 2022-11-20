package com.github.davenury.test

import com.github.davenury.common.AddUserChange
import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.Notification
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.tests.Changes
import com.github.davenury.tests.strategies.RandomPeersWithDelayOnConflictStrategy
import io.mockk.*
import kotlinx.coroutines.*
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.isEqualTo
import strikt.assertions.isSuccess
import java.util.concurrent.Executors
import java.util.concurrent.Phaser
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock

class ChangesTest {

    companion object {
        private val peers: Map<Int, List<String>> = mapOf(
            0 to listOf("localhost0:8080", "localhost0:8081", "localhost0:8082"),
            1 to listOf("localhost1:8080"),
            2 to listOf("localhost2:8080"),
            3 to listOf("localhost3:8080", "localhost3:8081", "localhost3:8082"),
            4 to listOf("localhost4:8080"),
            5 to listOf("localhost5:8080"),
        )
    }

    @Disabled("Infinite test - TODO fix before closing PR")
    @Test
    fun `should throw AssertionError, when list of available peersets is empty`(): Unit = runBlocking {
        // given - changes
        val sender = DummySender(peers, shouldNotify = false)
        val lockMock = mockk<Lock>()
        val conditionMock = mockk<Condition>()
        val subject = Changes(peers, sender, RandomPeersWithDelayOnConflictStrategy((0 until peers.size), lockMock, conditionMock))
        sender.setChanges(subject)
        val phaser = Phaser(peers.keys.size)
        phaser.register()

        every { conditionMock.await() } just Runs
        every { conditionMock.signalAll() } just Runs
        every { lockMock.lock() } just Runs
        every { lockMock.unlock() } just Runs

        // when - executing changes without notyfing about ending
        // after this all peersets are occupied with a change
        peers.keys.forEach { _ ->
            launch {
                subject.introduceChange(1)
                phaser.arrive()
            }
        }

        phaser.arriveAndAwaitAdvance()

        // and - another change
        val phase2Phaser = Phaser(2)
        launch {
            subject.introduceChange(1)
        }

        verify(exactly = 1) { conditionMock.await() }

        subject.handleNotification(
            Notification(
                AddUserChange(
                    "parentId",
                    "userName",
                    peers = listOf("localhost2:8080")
                ), result = ChangeResult(ChangeResult.Status.SUCCESS)
            )
        )

        verify(exactly = 1) { conditionMock.signalAll() }
    }

    @Test
    fun `should be able to unlock peersets`(): Unit = runBlocking {
        val sender = DummySender(peers, shouldNotify = false)
        val subject = Changes(peers, sender, RandomPeersWithDelayOnConflictStrategy((0 until peers.size)))
        sender.setChanges(subject)
        var change: Change? = null

        val phaser = Phaser(2)

        // when - executing changes without notyfing about ending
        // after this all peersets are occupied with a change
        peers.keys.forEach { key ->
            withContext(Dispatchers.IO) {
                if (key == 0) {
                    change = subject.introduceChange(1)
                    phaser.arrive()
                } else {
                    subject.introduceChange(1)
                }
            }
        }

        phaser.arriveAndAwaitAdvance()

        // and - we explicitly unlock one peer
        sender.notify(change!!.withAddress(peers[0]!!.first()))

        // then - should throw exception when adding another change
        expectCatching {
            subject.introduceChange(1)
        }.isSuccess()
    }

    @Test
    fun `should be able to execute changes without any errors and with valid parents ids`(): Unit = runBlocking {
        val changesToExecute = 10
        val phaser = Phaser(changesToExecute)
        phaser.register()
        val sender = DummySender(peers, shouldNotify = true, phaser = phaser)
        val subject = Changes(peers, sender, RandomPeersWithDelayOnConflictStrategy((0 until peers.size)))
        sender.setChanges(subject)

        // max 3 threads as we have 6 possible peersets and we cannot execute change to all of them plus 1
        val ctx = Executors.newFixedThreadPool(3).asCoroutineDispatcher()

        for (i in (1..changesToExecute)) {
            launch(ctx) {
                subject.introduceChange(2)
            }
        }

        phaser.arriveAndAwaitAdvance()

        val changes = sender.appearedChanges
        expectThat(areChangesValid(changes)).isEqualTo(true)
    }

    private fun areChangesValid(changes: List<Pair<String, Change>>): Boolean {
        val mapOfChanges = (0..changes.size).associateWith { InitialHistoryEntry.getId() }.toMutableMap()
        changes.forEach { (address, change) ->
            val peersetId = address.extractPeersetIdFromAddress()
            if (change.parentId != mapOfChanges[peersetId]) {
                println("ParentId differs for change in peerset $peersetId - parentId: ${change.parentId}, expected parent id: ${mapOfChanges[peersetId]}")
                return false
            }
            (change.peers.map { it.extractPeersetIdFromAddress() } + peersetId)
                .forEach { mapOfChanges[it] = change.toHistoryEntry().getId() }
        }
        return true
    }

    private fun String.extractPeersetIdFromAddress(): Int =
        this.split(":").first().chunked(1).last().toInt()
}