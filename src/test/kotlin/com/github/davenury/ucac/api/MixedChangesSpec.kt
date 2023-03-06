package com.github.davenury.ucac.api

import com.github.davenury.common.*
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.testHttpClient
import com.github.davenury.ucac.utils.IntegrationTestBase
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.davenury.ucac.utils.arriveAndAwaitAdvanceWithTimeout
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.*
import org.junit.jupiter.api.extension.ExtendWith
import org.slf4j.LoggerFactory
import strikt.api.expectThat
import strikt.assertions.*
import java.io.File
import java.util.concurrent.Phaser

@Suppress("HttpUrlsUsage")
@ExtendWith(TestLogExtension::class)
class MixedChangesSpec : IntegrationTestBase() {
    companion object {
        private val logger = LoggerFactory.getLogger(MultiplePeersetSpec::class.java)
    }

    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "application-integration.conf")
    }


    @Test
    fun `try to execute two following changes in the same time, first GPAC, then Raft`(): Unit = runBlocking {
        val change = change(0, 1)
        val secondChange = change(mapOf(0 to change.toHistoryEntry(0).getId()))

        val applyEndPhaser = Phaser(6)
        val beforeSendingApplyPhaser = Phaser(1)
        val electionPhaser = Phaser(4)
        val applyConsensusPhaser = Phaser(2)

        listOf(applyEndPhaser, electionPhaser, beforeSendingApplyPhaser, applyConsensusPhaser)
            .forEach { it.register() }
        val leaderElected = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            electionPhaser.arrive()
        }

        val signalListenersForCohort = mapOf(
            Signal.OnHandlingApplyEnd to SignalListener {
                logger.info("Arrived: ${it.subject.getPeerName()}")
                applyEndPhaser.arrive()
            },
            Signal.ConsensusLeaderElected to leaderElected,
            Signal.BeforeSendingApply to SignalListener {
                beforeSendingApplyPhaser.arrive()
            },
            Signal.ConsensusFollowerChangeAccepted to SignalListener {
                if (it.change?.id == secondChange.id) applyConsensusPhaser.arrive()
            }
        )

        apps = TestApplicationSet(
            listOf(3, 3),
            signalListeners = (0..5).associateWith { signalListenersForCohort }
        )

        val peers = apps.getPeers()

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async", change)

        beforeSendingApplyPhaser.arriveAndAwaitAdvanceWithTimeout()

        executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async", secondChange)

        applyEndPhaser.arriveAndAwaitAdvanceWithTimeout()

        applyConsensusPhaser.arriveAndAwaitAdvanceWithTimeout()


//      First peerset
        askAllForChanges(peers.filter { it.key.peersetId == 0 }.values).forEach {
            val changes = it.second
            expectThat(changes.size).isGreaterThanOrEqualTo(2)
            expectThat(changes[0]).isEqualTo(change)
            expectThat(changes[1]).isEqualTo(secondChange)
        }

        askAllForChanges(peers.filter { it.key.peersetId == 1 }.values).forEach {
            val changes = it.second
            expectThat(changes.size).isGreaterThanOrEqualTo(1)
            expectThat(changes[0]).isEqualTo(change)
        }
    }

    @Test
    fun `try to execute two following changes in the same time (two different peers), first GPAC, then Raft`(): Unit =
        runBlocking {
            val change = change(0, 1)
            val secondChange = change(mapOf(1 to change.toHistoryEntry(0).getId()))

            val applyEndPhaser = Phaser(6)
            val beforeSendingApplyPhaser = Phaser(1)
            val electionPhaser = Phaser(4)
            val applyConsensusPhaser = Phaser(3)

            listOf(applyEndPhaser, electionPhaser, beforeSendingApplyPhaser)
                .forEach { it.register() }
            val leaderElected = SignalListener {
                logger.info("Arrived ${it.subject.getPeerName()}")
                electionPhaser.arrive()
            }

            val signalListenersForCohort = mapOf(
                Signal.OnHandlingApplyEnd to SignalListener {
                    logger.info("Arrived: ${it.subject.getPeerName()}")
                    applyEndPhaser.arrive()
                },
                Signal.ConsensusLeaderElected to leaderElected,
                Signal.BeforeSendingApply to SignalListener {
                    beforeSendingApplyPhaser.arrive()
                },
                Signal.ConsensusFollowerChangeAccepted to SignalListener {
                    if (it.change?.id == secondChange.id) applyConsensusPhaser.arrive()
                }
            )

            apps = TestApplicationSet(
                listOf(3, 3),
                signalListeners = (0..5).associateWith { signalListenersForCohort }
            )

            val peers = apps.getPeers()

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            // when - executing transaction
            executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async", change)

            beforeSendingApplyPhaser.arriveAndAwaitAdvanceWithTimeout()

            executeChange("http://${apps.getPeer(1, 0).address}/v2/change/async", secondChange)

            applyEndPhaser.arriveAndAwaitAdvanceWithTimeout()

            applyConsensusPhaser.arriveAndAwaitAdvanceWithTimeout()

//      First peerset
            askAllForChanges(peers.filter { it.key.peersetId == 0 }.values).forEach {
                val changes = it.second
                expectThat(changes.size).isGreaterThanOrEqualTo(1)
                expectThat(changes[0]).isEqualTo(change)
            }

            askAllForChanges(peers.filter { it.key.peersetId == 1 }.values).forEach {
                val changes = it.second
                expectThat(changes.size).isGreaterThanOrEqualTo(2)
                expectThat(changes[0]).isEqualTo(change)
                expectThat(changes[1]).isEqualTo(secondChange)
            }
        }

    @Test
    fun `try to execute two following changes in the same time, first 2PC, then Raft`(): Unit = runBlocking {
        val firstChange = change(0, 1)
        val secondChange = change(mapOf(0 to firstChange.toHistoryEntry(0).getId()))
        val thirdChange = change(mapOf(1 to firstChange.toHistoryEntry(1).getId()))


        val applyEndPhaser = Phaser(1)
        val beforeSendingApplyPhaser = Phaser(1)
        val electionPhaser = Phaser(4)
        val applySecondChangePhaser = Phaser(2)
        val applyThirdChangePhaser = Phaser(2)

        listOf(applyEndPhaser, electionPhaser, beforeSendingApplyPhaser, applySecondChangePhaser, applyThirdChangePhaser)
            .forEach { it.register() }
        val leaderElected = SignalListener {
            logger.info("Arrived ${it.subject.getPeerName()}")
            electionPhaser.arrive()
        }

        val signalListenersForCohort = mapOf(
            Signal.TwoPCOnChangeApplied to SignalListener {
                logger.info("Arrived: ${it.subject.getPeerName()}")
                applyEndPhaser.arrive()
            },
            Signal.ConsensusLeaderElected to leaderElected,
            Signal.TwoPCOnChangeAccepted to SignalListener {
                beforeSendingApplyPhaser.arrive()
            },
            Signal.ConsensusFollowerChangeAccepted to SignalListener {
                if (it.change?.id == secondChange.id) applySecondChangePhaser.arrive()
                if (it.change?.id == thirdChange.id) applyThirdChangePhaser.arrive()
            }
        )

        apps = TestApplicationSet(
            listOf(3, 3),
            signalListeners = (0..5).associateWith { signalListenersForCohort }
        )

        val peers = apps.getPeers()

        electionPhaser.arriveAndAwaitAdvanceWithTimeout()

        // when - executing transaction
        executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async?use_2pc=true", firstChange)

        beforeSendingApplyPhaser.arriveAndAwaitAdvanceWithTimeout()

        executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async", secondChange)
        executeChange("http://${apps.getPeer(1, 1).address}/v2/change/async", thirdChange)

        applyEndPhaser.arriveAndAwaitAdvanceWithTimeout()

        applySecondChangePhaser.arriveAndAwaitAdvanceWithTimeout()
        applyThirdChangePhaser.arriveAndAwaitAdvanceWithTimeout()


//      First peerset
        askAllForChanges(peers.filter { it.key.peersetId == 0 }.values).forEach {
            val changes = it.second
            expectThat(changes.size).isGreaterThanOrEqualTo(3)
            expectThat(changes[1].id).isEqualTo(firstChange.id)
            expectThat(changes[2].id).isEqualTo(secondChange.id)
        }

        askAllForChanges(peers.filter { it.key.peersetId == 1 }.values).forEach {
            val changes = it.second
            expectThat(changes.size).isGreaterThanOrEqualTo(3)
            expectThat(changes[1].id).isEqualTo(firstChange.id)
            expectThat(changes[2].id).isEqualTo(thirdChange.id)
        }
    }

    @Test
    fun `try to execute two following changes in the same time (two different peers), first 2PC, then Raft`(): Unit =
        runBlocking {
            val change = change(0, 1)
            val secondChange = change(mapOf(1 to change.toHistoryEntry(0).getId()))

            val beforeSendingApplyPhaser = Phaser(1)
            val applyEndPhaser = Phaser(1)
            val electionPhaser = Phaser(4)
            val applyConsensusPhaser = Phaser(2)
            val apply2PCPhaser = Phaser(4)
            listOf(applyEndPhaser, electionPhaser, beforeSendingApplyPhaser, applyConsensusPhaser, apply2PCPhaser)
                .forEach { it.register() }

            val leaderElected = SignalListener {
                electionPhaser.arrive()
            }

            val signalListenersForCohort = mapOf(
                Signal.TwoPCOnChangeApplied to SignalListener {
                    applyEndPhaser.arrive()
                },
                Signal.ConsensusLeaderElected to leaderElected,
                Signal.TwoPCOnChangeAccepted to SignalListener {
                    beforeSendingApplyPhaser.arrive()
                },
                Signal.ConsensusFollowerChangeAccepted to SignalListener {
                    if (it.change?.id == change.id){
                        apply2PCPhaser.arrive()
                    }
                    if (it.change?.id == secondChange.id) {
                        applyConsensusPhaser.arrive()
                    }
                }
            )

            apps = TestApplicationSet(
                listOf(3, 3),
                signalListeners = (0..5).associateWith { signalListenersForCohort }
            )

            val peers = apps.getPeers()

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            // when - executing transaction
            executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async?use_2pc=true", change)

            beforeSendingApplyPhaser.arriveAndAwaitAdvanceWithTimeout()

            executeChange("http://${apps.getPeer(1, 0).address}/v2/change/async", secondChange)

            applyEndPhaser.arriveAndAwaitAdvanceWithTimeout()

            applyConsensusPhaser.arriveAndAwaitAdvanceWithTimeout()

            apply2PCPhaser.arriveAndAwaitAdvanceWithTimeout()

//      First peerset
            askAllForChanges(peers.filter { it.key.peersetId == 0 }.values).forEach {
                val changes = it.second
                expectThat(changes.size).isGreaterThanOrEqualTo(2)
                expectThat(changes[1].id).isEqualTo(change.id)
            }

            askAllForChanges(peers.filter { it.key.peersetId == 1 }.values).forEach {
                val changes = it.second
                expectThat(changes.size).isGreaterThanOrEqualTo(3)
                expectThat(changes[1].id).isEqualTo(change.id)
                expectThat(changes[2].id).isEqualTo(secondChange.id)
            }
        }


    @Test
    fun `try to execute two following changes, first 2PC, then Raft`(): Unit =
        runBlocking {
            val firstChange = change(0, 1)
            val secondChange = change(mapOf(1 to firstChange.toHistoryEntry(1).getId()))
            val thirdChange = change(mapOf(0 to firstChange.toHistoryEntry(0).getId()))


            val applyEndPhaser = Phaser(1)
            val electionPhaser = Phaser(4)
            val applySecondChangePhaser = Phaser(2)
            val applyThirdChangePhaser = Phaser(2)

            listOf(applyEndPhaser,applyThirdChangePhaser,applySecondChangePhaser, electionPhaser)
                .forEach { it.register() }
            val leaderElected = SignalListener {
                electionPhaser.arrive()
            }

            val signalListenersForCohort = mapOf(
                Signal.TwoPCOnChangeApplied to SignalListener {
                    applyEndPhaser.arrive()
                },
                Signal.ConsensusLeaderElected to leaderElected,
                Signal.ConsensusFollowerChangeAccepted to SignalListener {
                    if (it.change?.id == thirdChange.id) {
                        applyThirdChangePhaser.arrive()
                    }
                    if (it.change?.id == secondChange.id) {
                        applySecondChangePhaser.arrive()
                    }
                }
            )

            apps = TestApplicationSet(
                listOf(3, 3),
                signalListeners = (0..5).associateWith { signalListenersForCohort }
            )

            val peers = apps.getPeers()

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            // when - executing transaction
            executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async?use_2pc=true", firstChange)

            applyEndPhaser.arriveAndAwaitAdvanceWithTimeout()

            executeChange("http://${apps.getPeer(1, 0).address}/v2/change/async", secondChange)
            executeChange("http://${apps.getPeer(0, 0).address}/v2/change/async", thirdChange)
            applySecondChangePhaser.arriveAndAwaitAdvanceWithTimeout()
            applyThirdChangePhaser.arriveAndAwaitAdvanceWithTimeout()

//      First peerset
            askAllForChanges(peers.filter { it.key.peersetId == 0 }.values).forEach {
                val changes = it.second
                expectThat(changes.size).isEqualTo(3)
                expectThat(changes[1].id).isEqualTo(firstChange.id)
                expectThat(changes[2].id).isEqualTo(thirdChange.id)
            }

            askAllForChanges(peers.filter { it.key.peersetId == 1 }.values).forEach {
                val changes = it.second
                expectThat(changes.size).isEqualTo(3)
                expectThat(changes[1].id).isEqualTo(firstChange.id)
                expectThat(changes[2].id).isEqualTo(secondChange.id)
            }
        }

    private suspend fun executeChange(uri: String, change: Change): HttpResponse =
        testHttpClient.post(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private suspend fun askForChanges(peerAddress: PeerAddress) =
        testHttpClient.get<Changes>("http://${peerAddress.address}/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

    private suspend fun askAllForChanges(peerAddresses: Collection<PeerAddress>) =
        peerAddresses.map { Pair(it, askForChanges(it)) }

    private fun change(vararg peersetIds: Int) = AddUserChange(
        "userName",
        peersets = peersetIds.map {
            ChangePeersetInfo(it, InitialHistoryEntry.getId())
        },
    )

    private fun change(peerSetIdToId: Map<Int, String>) = AddUserChange(
        "userName",
        peersets = peerSetIdToId.map { ChangePeersetInfo(it.key, it.value) },
    )
}