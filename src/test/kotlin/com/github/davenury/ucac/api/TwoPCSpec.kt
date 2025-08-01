package com.github.davenury.ucac.api

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeCreationResponse
import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.Changes
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.common.StandardChange
import com.github.davenury.common.TwoPCChange
import com.github.davenury.common.TwoPCStatus
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.common.structure.CodeSubscriber
import com.github.davenury.ucac.common.structure.Subscribers
import com.github.davenury.ucac.testHttpClient
import com.github.davenury.ucac.utils.IntegrationTestBase
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestApplicationSet.Companion.NON_RUNNING_PEER
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.davenury.ucac.utils.arriveAndAwaitAdvanceWithTimeout
import io.ktor.client.call.body
import io.ktor.client.plugins.ServerResponseException
import io.ktor.client.request.accept
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.fail
import org.junitpioneer.jupiter.RetryingTest
import org.slf4j.LoggerFactory
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.contains
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isGreaterThanOrEqualTo
import strikt.assertions.isLessThanOrEqualTo
import strikt.assertions.isNotNull
import strikt.assertions.isSuccess
import strikt.assertions.isTrue
import java.time.Duration
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.set
import kotlin.system.measureTimeMillis

@Suppress("HttpUrlsUsage")
@ExtendWith(TestLogExtension::class)
class TwoPCSpec : IntegrationTestBase() {
    companion object {
        private val logger = LoggerFactory.getLogger(TwoPCSpec::class.java)
    }

    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "application-integration.conf")
    }

    @Test
    fun `should execute transaction in every peer from every of two peersets`(): Unit =
        runBlocking {
            val changeAppliedPhaser = Phaser(8)
            val electionPhaser = Phaser(4)
            listOf(changeAppliedPhaser, electionPhaser).forEach { it.register() }

            val signalListenersForCohort =
                mapOf(
                    Signal.ConsensusFollowerChangeAccepted to
                        SignalListener {
                            changeAppliedPhaser.arrive()
                        },
                    Signal.ConsensusLeaderElected to SignalListener { electionPhaser.arrive() },
                )

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2"),
                        "peerset1" to listOf("peer3", "peer4", "peer5"),
                    ),
                    signalListeners = (0..5).map { "peer$it" }.associateWith { signalListenersForCohort },
                )

            val change = change(0, 1)

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            // when - executing transaction
            executeChange(
                "http://${apps.getPeer("peer0").address}/v2/change/async?peerset=peerset0&use_2pc=true",
                change,
            )

            changeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

            askAllForChanges("peerset0", "peerset1").forEach { changes ->
                expectThat(changes.size).isEqualTo(2)
                expectThat(changes[0]).isA<TwoPCChange>()
                    .with(TwoPCChange::twoPCStatus) { isEqualTo(TwoPCStatus.ACCEPTED) }
                    .with(TwoPCChange::change) { isEqualTo(change) }
                expectThat(changes[1]).isA<StandardChange>()
                    .with(StandardChange::content) { isEqualTo("change") }
            }
        }

    @Test
    fun `1000 change processed sequentially`(): Unit =
        runBlocking {
            val peersWithoutLeader = 4

            val leaderElectedPhaser = Phaser(peersWithoutLeader)
            leaderElectedPhaser.register()

            val phaser = Phaser(peersWithoutLeader * 2)
            phaser.register()

            val peerLeaderElected =
                SignalListener {
                    logger.info("Arrived ${it.subject.getPeerName()}")
                    leaderElectedPhaser.arrive()
                }

            val peerChangeAccepted =
                SignalListener {
                    logger.info("Arrived change: ${it.change}")
                    phaser.arrive()
                }

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2"),
                        "peerset1" to listOf("peer3", "peer4", "peer5"),
                    ),
                    signalListeners =
                        (0..5).map { "peer$it" }.associateWith {
                            mapOf(
                                Signal.ConsensusLeaderElected to peerLeaderElected,
                                Signal.ConsensusFollowerChangeAccepted to peerChangeAccepted,
                            )
                        },
                )

            leaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()
            logger.info("Leader elected")

            var change = change(0, 1)

            val endRange = 1000

            var time = 0L

            repeat(endRange) {
                time +=
                    measureTimeMillis {
                        expectCatching {
                            executeChangeSync("peer0", "peerset0", change, true)
                        }.isSuccess()
                    }
                phaser.arriveAndAwaitAdvanceWithTimeout()
                change = twoPeersetChange(change)
            }
            // when: peer1 executed change

            expectThat(time / endRange).isLessThanOrEqualTo(500L)

            askAllForChanges("peerset0").forEach { changes ->
                // then: there are two changes
                expectThat(changes.size).isEqualTo(endRange * 2)
                expectThat(
                    changes.all { it is TwoPCChange && it.twoPCStatus == TwoPCStatus.ACCEPTED || (it as StandardChange).content == "change" },
                ).isTrue()
            }
        }

    @Test
    fun `should be able to execute 2 transactions`(): Unit =
        runBlocking {
            val changeAppliedPhaser = Phaser(4)
            val changeSecondAppliedPhaser = Phaser(4)
            val electionPhaser = Phaser(4)
            listOf(changeAppliedPhaser, changeSecondAppliedPhaser, electionPhaser).forEach { it.register() }

            val change = change(0, 1)
            val changeSecond = change(Pair(0, null), Pair(1, null))

            val signalListenersForCohort =
                mapOf(
                    Signal.ConsensusFollowerChangeAccepted to
                        SignalListener {
                            if (it.change!!.id == change.id) changeAppliedPhaser.arrive()
                            if (it.change!!.id == changeSecond.id) changeSecondAppliedPhaser.arrive()
                        },
                    Signal.ConsensusLeaderElected to SignalListener { electionPhaser.arrive() },
                )

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2"),
                        "peerset1" to listOf("peer3", "peer4", "peer5"),
                    ),
                    signalListeners = (0..5).map { "peer$it" }.associateWith { signalListenersForCohort },
                )

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            // when - executing transaction
            executeChange(
                "http://${apps.getPeer("peer0").address}/v2/change/async?peerset=peerset0&use_2pc=true",
                change,
            )

            changeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

            executeChange(
                "http://${apps.getPeer("peer0").address}/v2/change/async?peerset=peerset0&use_2pc=true",
                changeSecond,
            )

            changeSecondAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

            askAllForChanges("peerset0", "peerset1").forEach { changes ->
                expectThat(changes.size).isEqualTo(4)
                expectThat(changes[0]).isA<TwoPCChange>()
                    .with(TwoPCChange::twoPCStatus) { isEqualTo(TwoPCStatus.ACCEPTED) }
                    .with(TwoPCChange::change) { isEqualTo(change) }
                expectThat(changes[1]).isA<StandardChange>()
                    .with(StandardChange::content) { isEqualTo("change") }
                expectThat(changes[2]).isA<TwoPCChange>()
                    .with(TwoPCChange::twoPCStatus) { isEqualTo(TwoPCStatus.ACCEPTED) }
                    .with(TwoPCChange::change) { isEqualTo(changeSecond) }
                expectThat(changes[3]).isA<StandardChange>()
                    .with(StandardChange::content) { isEqualTo("change") }
            }
        }

    @Test
    fun `should not execute transaction if one peerset is not responding`(): Unit =
        runBlocking {
            val change2PCAppliedPhaser = Phaser(1)
            val changeRaftAppliedPhaser = Phaser(2)
            val applied2PCChangesListener =
                SignalListener {
                    logger.info("Arrived 2PC: ${it.subject.getPeerName()}")
                    change2PCAppliedPhaser.arrive()
                }

            val appliedRaftChangesListener =
                SignalListener {
                    if (it.change is TwoPCChange && (it.change as TwoPCChange).twoPCStatus == TwoPCStatus.ABORTED) {
                        logger.info("Arrived raft: ${it.subject.getPeerName()}")
                        changeRaftAppliedPhaser.arrive()
                    }
                }

            val electionPhaser = Phaser(2)
            val leaderElected =
                SignalListener {
                    electionPhaser.arrive()
                }

            listOf(electionPhaser, change2PCAppliedPhaser, changeRaftAppliedPhaser).forEach { it.register() }

            val signalListenersForCohort =
                mapOf(
                    Signal.TwoPCOnChangeApplied to applied2PCChangesListener,
                    Signal.ConsensusFollowerChangeAccepted to appliedRaftChangesListener,
                    Signal.ConsensusLeaderElected to leaderElected,
                )

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2"),
                        "peerset1" to listOf("peer3", "peer4", "peer5"),
                    ),
                    appsToExclude = listOf("peer3", "peer4", "peer5"),
                    signalListeners = (0..5).map { "peer$it" }.associateWith { signalListenersForCohort },
                )

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            val change: Change = change(0, 1)
            val result =
                executeChange(
                    "http://${apps.getPeer("peer0").address}/v2/change/async?peerset=peerset0&use_2pc=true",
                    change,
                )

            expectThat(result.status).isEqualTo(HttpStatusCode.Accepted)

            change2PCAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()
            changeRaftAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

            // then - transaction should not be executed
            askAllForChanges("peerset0").forEach { changes ->
                expectThat(changes.size).isEqualTo(2)
                expectThat(changes[0]).isA<TwoPCChange>()
                    .with(TwoPCChange::twoPCStatus) {
                        isEqualTo(TwoPCStatus.ACCEPTED)
                    }
                    .with(TwoPCChange::change) {
                        isEqualTo(change)
                    }
                expectThat(changes[1]).isA<TwoPCChange>()
                    .with(TwoPCChange::twoPCStatus) {
                        isEqualTo(TwoPCStatus.ABORTED)
                    }
                    .with(TwoPCChange::change) {
                        isEqualTo(change)
                    }
            }

            try {
                testHttpClient.get(
                    "http://${apps.getPeer("peer0").address}/v2/change_status/${change.id}?peerset=peerset0",
                ) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                }.body<HttpResponse>()
                fail("executing change didn't fail")
            } catch (e: ServerResponseException) {
                expect {
                    that(e.response.status).isEqualTo(HttpStatusCode.InternalServerError)
                    that(e.response.bodyAsText()).contains("Change was applied with ABORT result")
                }
            }
        }

    @Disabled("Servers are not able to stop here")
    @Test
    fun `transaction should not pass when more than half peers of any peerset aren't responding`(): Unit =
        runBlocking {
            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2"),
                        "peerset1" to listOf("peer3", "peer4", "peer5", "peer6", "peer7"),
                    ),
                    appsToExclude = listOf("peer2", "peer5", "peer6", "peer7"),
                )
            val change = change(0, 1)

            delay(5000)

            // when - executing transaction
            try {
                executeChangeSync("peer0", "peerset0", change, true)
                fail("Exception not thrown")
            } catch (e: Exception) {
                expectThat(e).isA<ServerResponseException>()
                expectThat(e.message!!).contains("Transaction failed due to too many retries of becoming a leader.")
            }

            // we need to wait for timeout from peers of second peerset
            delay(10000)

            // then - transaction should not be executed
            askAllForChanges("peerset0").forEach { changes ->
                expectThat(changes.size).isEqualTo(0)
            }
        }

    @Test
    fun `transaction should pass when more than half peers of all peersets are operative`(): Unit =
        runBlocking {
            val changeAppliedPhaser = Phaser(6)
            changeAppliedPhaser.register()

            val peerApplyCommitted =
                SignalListener {
                    logger.info("Arrived: ${it.subject.getPeerName()}")
                    changeAppliedPhaser.arrive()
                }

            val electionPhaser = Phaser(3)
            electionPhaser.register()
            val leaderElected =
                SignalListener {
                    electionPhaser.arrive()
                }

            val signalListenersForCohort =
                mapOf(
                    Signal.ConsensusFollowerChangeAccepted to peerApplyCommitted,
                    Signal.ConsensusLeaderElected to leaderElected,
                )

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2"),
                        "peerset1" to listOf("peer3", "peer4", "peer5", "peer6", "peer7"),
                    ),
                    appsToExclude = listOf("peer2", "peer6", "peer7"),
                    signalListeners = (0..7).map { "peer$it" }.associateWith { signalListenersForCohort },
                )
            val change: Change = change(0, 1)

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            // when - executing transaction
            executeChangeSync("peer0", "peerset0", change, true)

            changeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

            // then - transaction should be executed in every peerset
            askAllRunningPeersForChanges("peerset0", "peerset1").forEach { changes ->
                expectThat(changes.size).isEqualTo(2)
                expectThat(changes[0]).isA<TwoPCChange>()
                    .with(Change::peersets) { isEqualTo(change.peersets) }
                    .with(TwoPCChange::twoPCStatus) { isEqualTo(TwoPCStatus.ACCEPTED) }
                    .with(TwoPCChange::change) { isEqualTo(change) }
                expectThat(changes[1]).isA<StandardChange>()
                    .with(StandardChange::content) { isEqualTo("change") }
            }
        }

    @RetryingTest(3)
    fun `transaction should be processed if peer from second peerset accepted change`(): Unit =
        runBlocking {
            val firstPeersetChangeAppliedPhaser = Phaser(4)
            val secondPeersetChangeAppliedPhaser = Phaser(8)
            val electionPhaser = Phaser(6)
            val leaderElected =
                SignalListener {
                    electionPhaser.arrive()
                }
            listOf(
                firstPeersetChangeAppliedPhaser,
                secondPeersetChangeAppliedPhaser,
                electionPhaser,
            ).forEach { it.register() }

            val isChangeNotAccepted = AtomicBoolean(true)

            val onHandleDecision =
                SignalListener {
                    if (isChangeNotAccepted.get()) throw Exception("Simulate ignoring 2PC-decision message")
                }

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2"),
                        "peerset1" to listOf("peer3", "peer4", "peer5", "peer6", "peer7"),
                    ),
                    signalListeners =
                        (0..2).map { "peer$it" }.associateWith {
                            mapOf(
                                Signal.ConsensusLeaderElected to leaderElected,
                                Signal.ConsensusFollowerChangeAccepted to
                                    SignalListener {
                                        logger.info("Change accepted (first): ${it.subject}")
                                        firstPeersetChangeAppliedPhaser.arrive()
                                    },
                            )
                        }.toMap() +
                            (3..7).map { "peer$it" }.associateWith {
                                mapOf(
                                    Signal.TwoPCOnHandleDecision to onHandleDecision,
                                    Signal.ConsensusLeaderElected to leaderElected,
                                    Signal.ConsensusFollowerChangeAccepted to
                                        SignalListener {
                                            logger.info("Change accepted (second): ${it.subject}")
                                            secondPeersetChangeAppliedPhaser.arrive()
                                        },
                                )
                            }.toMap(),
                )
            val change: Change = change(0, 1)

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            executeChangeSync("peer0", "peerset0", change, true)

            firstPeersetChangeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

            isChangeNotAccepted.set(false)

            secondPeersetChangeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

            askAllForChanges("peerset0", "peerset1").forEach { changes ->
                expectThat(changes.size).isEqualTo(2)
                expectThat(changes[0]).isA<TwoPCChange>()
                    .with(TwoPCChange::peersets) {
                        isEqualTo(change.peersets)
                    }
                    .with(TwoPCChange::twoPCStatus) {
                        isEqualTo(TwoPCStatus.ACCEPTED)
                    }
                    .with(TwoPCChange::change) {
                        isEqualTo(change)
                    }
                expectThat(changes[1]).isA<StandardChange>()
                    .with(StandardChange::content) { isEqualTo("change") }
            }
        }

    @Disabled("I am not sure if this is solid case in 2PC")
    @Test
    fun `transaction should be processed if leader fails after ft-agree`(): Unit =
        runBlocking {
            val failAction =
                SignalListener {
                    throw RuntimeException("Leader failed after ft-agree")
                }

            val applyCommittedPhaser = Phaser(7)
            applyCommittedPhaser.register()

            val peerApplyCommitted =
                SignalListener {
                    logger.info("Arrived: ${it.subject.getPeerName()}")
                    applyCommittedPhaser.arrive()
                }

            val signalListenersForLeaders =
                mapOf(
                    Signal.BeforeSendingApply to failAction,
                )
            val signalListenersForCohort =
                mapOf(
                    Signal.OnHandlingApplyCommitted to peerApplyCommitted,
                )

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2"),
                        "peerset1" to listOf("peer3", "peer4", "peer5", "peer6", "peer7"),
                    ),
                    signalListeners =
                        mapOf(
                            "peer0" to signalListenersForLeaders,
                            "peer1" to signalListenersForCohort,
                            "peer2" to signalListenersForCohort,
                            "peer3" to signalListenersForCohort,
                            "peer4" to signalListenersForCohort,
                            "peer5" to signalListenersForCohort,
                            "peer6" to signalListenersForCohort,
                            "peer7" to signalListenersForCohort,
                        ),
                )
            val change = change(0, 1)

            // when - executing transaction something should go wrong after ft-agree
            expectThrows<ServerResponseException> {
                executeChangeSync("peer0", "peerset0", change, true)
            }

            applyCommittedPhaser.arriveAndAwaitAdvanceWithTimeout()

            askAllForChanges("peerset0", "peerset1").forEach { changes ->
                expectThat(changes.size).isGreaterThanOrEqualTo(1)
                expectThat(changes[0]).isEqualTo(change)
            }
        }

    @Test
    fun `should be able to execute change in two different peersets even if changes in peersets are different`() =
        runBlocking {
            val consensusLeaderElectedPhaser = Phaser(6)
            val firstChangePhaser = Phaser(2)
            val secondChangePhaser = Phaser(4)
            val finalChangePhaser = Phaser(6)

            listOf(firstChangePhaser, secondChangePhaser, finalChangePhaser, consensusLeaderElectedPhaser)
                .forEach { it.register() }

            val firstChangeListener =
                SignalListener {
                    if (it.change is StandardChange && (it.change as StandardChange).content == "first") {
                        firstChangePhaser.arrive()
                    } else if (it.change is StandardChange && (it.change as StandardChange).content == "third") {
                        finalChangePhaser.arrive()
                    }
                }

            val secondChangeListener =
                SignalListener {
                    if (it.change is StandardChange && (it.change as StandardChange).content == "second") {
                        secondChangePhaser.arrive()
                    } else if (it.change is StandardChange && (it.change as StandardChange).content == "third") {
                        finalChangePhaser.arrive()
                    }
                }

            val leaderElectedListener =
                SignalListener {
                    consensusLeaderElectedPhaser.arrive()
                }

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2"),
                        "peerset1" to listOf("peer3", "peer4", "peer5", "peer6", "peer7"),
                    ),
                    signalListeners =
                        List(3) {
                            "peer$it" to
                                mapOf(
                                    Signal.ConsensusFollowerChangeAccepted to firstChangeListener,
                                    Signal.ConsensusLeaderElected to leaderElectedListener,
                                )
                        }.toMap() +
                            List(5) {
                                "peer${it + 3}" to
                                    mapOf(
                                        Signal.ConsensusFollowerChangeAccepted to secondChangeListener,
                                        Signal.ConsensusLeaderElected to leaderElectedListener,
                                    )
                            }.toMap(),
                )

            consensusLeaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()

            // given - change in first peerset
            val firstChange =
                StandardChange(
                    "first",
                    peersets = listOf(ChangePeersetInfo(PeersetId("peerset0"), InitialHistoryEntry.getId())),
                )
            expectCatching {
                executeChangeSync("peer0", "peerset0", firstChange)
            }.isSuccess()

            firstChangePhaser.arriveAndAwaitAdvanceWithTimeout()

            // and - change in second peerset
            val secondChange =
                StandardChange(
                    "second",
                    peersets = listOf(ChangePeersetInfo(PeersetId("peerset1"), InitialHistoryEntry.getId())),
                )
            expectCatching {
                executeChangeSync("peer3", "peerset1", secondChange)
            }.isSuccess()

            secondChangePhaser.arriveAndAwaitAdvanceWithTimeout()

            // when - executing change between two peersets
            val lastChange: Change =
                StandardChange(
                    "third",
                    peersets =
                        listOf(
                            ChangePeersetInfo(
                                PeersetId("peerset0"),
                                firstChange.toHistoryEntry(PeersetId("peerset0")).getId(),
                            ),
                            ChangePeersetInfo(
                                PeersetId("peerset1"),
                                secondChange.toHistoryEntry(PeersetId("peerset1")).getId(),
                            ),
                        ),
                )

            expectCatching {
                executeChangeSync("peer0", "peerset0", lastChange, true)
            }.isSuccess()

            finalChangePhaser.arriveAndAwaitAdvanceWithTimeout()

//          First peerset
            askAllForChanges("peerset0").forEach {
                expectThat(it.size).isEqualTo(3)
                expectThat(it[0]).isEqualTo(firstChange)
                expectThat(it[1]).isA<TwoPCChange>()
                    .with(TwoPCChange::change) { isEqualTo(lastChange) }
                    .with(TwoPCChange::twoPCStatus) { isEqualTo(TwoPCStatus.ACCEPTED) }
                expectThat(it[2]).isA<StandardChange>()
                    .with(StandardChange::content) { isEqualTo("third") }
            }

            askAllForChanges("peerset1").forEach {
                expectThat(it.size).isEqualTo(3)
                expectThat(it[0]).isEqualTo(secondChange)
                expectThat(it[1]).isA<TwoPCChange>()
                    .with(TwoPCChange::change) { isEqualTo(lastChange) }
                    .with(TwoPCChange::twoPCStatus) { isEqualTo(TwoPCStatus.ACCEPTED) }
                expectThat(it[2]).isA<StandardChange>()
                    .with(StandardChange::content) { isEqualTo("third") }
            }
        }

    @Test
    fun `atomic commitment between one-peer peersets`(): Unit =
        runBlocking {
            val consensusLeaderPhaser = Phaser(3)
            val signalListener =
                SignalListener {
                    consensusLeaderPhaser.arrive()
                }
            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0"),
                        "peerset1" to listOf("peer1"),
                    ),
                    signalListeners =
                        (0..1).map {
                            "peer$it"
                        }.associateWith { mapOf(Signal.ConsensusLeaderIHaveBeenElected to signalListener) },
                )

            consensusLeaderPhaser.arriveAndAwaitAdvanceWithTimeout()

            expectCatching {
                val change1 = change(0, 1)
                val change2 = twoPeersetChange(change1)
                executeChangeSync("peer0", "peerset0", change1, true)
                executeChangeSync("peer0", "peerset0", change2, true)
            }.isSuccess()

            askAllForChanges("peerset0").forEach { changes ->
                expectThat(changes.size).isEqualTo(4)
            }
        }

    // Missing sync after change for other peers
    @RetryingTest(2)
    fun `2pc on multiple peersets`(): Unit =
        runBlocking {
            val leaderElectedPhaser = Phaser(5)

            fun subscribers() =
                Subscribers().apply {
                    this.registerSubscriber(
                        CodeSubscriber { _, _ ->
                            leaderElectedPhaser.arrive()
                        },
                    )
                }

            val peers =
                mapOf(
                    "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    "peerset1" to listOf("peer1", "peer2", "peer4"),
                    "peerset2" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    "peerset3" to listOf("peer2", "peer3"),
                )
            val reversedPeers: MutableMap<String, MutableMap<PeersetId, Subscribers>> = mutableMapOf()
            peers.forEach { (peersetId, peers) ->
                peers.forEach { peer ->
                    reversedPeers[peer] =
                        reversedPeers.getOrDefault(peer, mutableMapOf()).apply {
                            this[PeersetId(peersetId)] = subscribers()
                        }
                }
            }

            apps =
                TestApplicationSet(
                    peers,
                    subscribers = reversedPeers,
                )

            leaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()

            val change01 = change(0, 1)
            val change23 = change(2, 3)

            logger.info("Sending change between 0 and 1")

            var change01Result: ChangeCreationResponse? = null
            expectCatching {
                change01Result = executeChangeSync("peer0", "peerset0", change01, true)
            }.isSuccess()

            var change23Result: ChangeCreationResponse? = null
            logger.info("Sending change between 2 and 3")
            expectCatching {
                change23Result = executeChangeSync("peer0", "peerset2", change23, true)
            }.isSuccess()

            val change12 =
                change(
                    1 to null,
                    2 to change23Result!!.entryId,
                )
            val change03 =
                change(
                    0 to change01Result!!.entryId,
                    3 to null,
                )

            logger.info("Sending change between 1 and 2")
            expectCatching {
                executeChangeSync("peer4", "peerset1", change12, true)
            }.isSuccess()

            logger.info("Sending change between 0 and 3")
            expectCatching {
                executeChangeSync("peer3", "peerset3", change03, true)
            }.isSuccess()

            val changes =
                listOf(
                    askForChanges(apps.getPeer("peer0"), "peerset0"),
                    askForChanges(apps.getPeer("peer0"), "peerset2"),
                    askForChanges(apps.getPeer("peer4"), "peerset1"),
                    askForChanges(apps.getPeer("peer3"), "peerset3"),
                )
            changes.forEach { ch ->
                expectThat(ch.size).isEqualTo(4)
            }
        }

    @Test
    fun `should be able to execute transaction to its end, when the leader fails`(): Unit =
        runBlocking {
            val consensusLeaderElectedPhaser = Phaser(5)
            val twoPcChangePhaser = Phaser(2)
            val newConsensusLeaderPhaser = Phaser(2)
            val changeAppliedPhaser = Phaser(2)

            val consensusLeaders: MutableMap<PeersetId, PeerId> = mutableMapOf()

            fun subscribers() =
                Subscribers().apply {
                    this.registerSubscriber(
                        CodeSubscriber { peerId, peersetId ->
                            logger.info("New consensus leader elected: $peerId")
                            if (consensusLeaders[peersetId] != null) {
                                newConsensusLeaderPhaser.arrive()
                            }
                            consensusLeaders[peersetId] = peerId
                        },
                    )
                }

            val leaderElectedListener =
                SignalListener {
                    consensusLeaderElectedPhaser.arrive()
                }

            val twoPCLeaderListener =
                SignalListener {
                    twoPcChangePhaser.arrive()
                    throw RuntimeException()
                }

            val changeAppliedListener =
                SignalListener {
                    changeAppliedPhaser.arrive()
                }

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2"),
                        "peerset1" to listOf("peer3", "peer4", "peer5"),
                    ),
                    subscribers =
                        (0..2).map { "peer$it" }.associateWith { mapOf(PeersetId("peerset0") to subscribers()) } +
                            (3..5).map { "peer$it" }.associateWith { mapOf(PeersetId("peerset1") to subscribers()) },
                    signalListeners =
                        (0..5).map { "peer$it" }.associateWith {
                            mapOf(
                                Signal.ConsensusLeaderElected to leaderElectedListener,
                                Signal.TwoPCOnChangeAccepted to twoPCLeaderListener,
                                Signal.TwoPCOnChangeApplied to changeAppliedListener,
                            )
                        },
                )

            // pick first consensus leader
            consensusLeaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()

            expectThat(consensusLeaders[PeersetId("peerset0")]).isNotNull()
            expectThat(consensusLeaders[PeersetId("peerset1")]).isNotNull()

            val firstConsensusLeader = consensusLeaders[PeersetId("peerset0")]!!

            // execute change that will stop at TwoPCChange(Accepted)
            expectCatching {
                val change1 = change(0, 1)
                executeChangeSync(firstConsensusLeader.toString(), "peerset0", change1, true)
            }.isFailure()

            twoPcChangePhaser.arriveAndAwaitAdvanceWithTimeout()

            logger.info("Killing - $firstConsensusLeader")
            // force peers to pick another consensus leader
            apps.stopApp(firstConsensusLeader)

            // make sure current change in peersets are TwoPCChanges - potential rc?
            expectThat(
                askForChanges(
                    apps.getPeer(
                        listOf("peer0", "peer1", "peer2")
                            .map { PeerId(it) }
                            .filterNot { it == firstConsensusLeader }
                            .first(),
                    ),
                    "peerset0",
                ).last(),
            ).isA<TwoPCChange>()
            expectThat(askForChanges(apps.getPeer(PeerId("peer3")), "peerset1").last()).isA<TwoPCChange>()

            newConsensusLeaderPhaser.arriveAndAwaitAdvanceWithTimeout()

            changeAppliedPhaser.arriveAndAwaitAdvanceWithTimeout()

            listOf(
                PeersetId("peerset0").let { Pair(consensusLeaders[it]!!, it) },
                PeersetId("peerset1").let { Pair(consensusLeaders[it]!!, it) },
            )
                .map { Pair(it.first, it.second) }
                .filterNot { it.first == firstConsensusLeader }
                .forEach {
                    val change = askForChanges(apps.getPeer(it.first), it.second.peersetId).last()
                    expectThat(change).isA<TwoPCChange>()
                    expectThat((change as TwoPCChange).twoPCStatus).isEqualTo(TwoPCStatus.ABORTED)
                }
        }

    @Test
    @Disabled("Sometimes consensus adds entry to history but doesn't send signal - bother Radek about it")
    fun `should be able to execute transaction to its end, even if the cohort fails`(): Unit =
        runBlocking {
            val consensusLeaderElectedPhaser = Phaser(5)
            val askForDecisionPhaser = Phaser(2)
            val finalPhaser = Phaser(3)
            val finalConsensusPhaser = Phaser(5)

            val leaderElectedListener =
                SignalListener {
                    consensusLeaderElectedPhaser.arrive()
                }

            val consensusLeaders: MutableMap<PeersetId, PeerId> = mutableMapOf()

            val changeWasAccepted = AtomicBoolean(false)
            val askedForDecision = AtomicBoolean(false)
            val changeHandleDecisionListener =
                SignalListener {
                    changeWasAccepted.set(true)
                    if (!changeWasAccepted.get()) {
                        throw RuntimeException()
                    }
                }

            val counters = mutableListOf<PeerId>()

            fun subscribers() =
                Subscribers().apply {
                    this.registerSubscriber(
                        CodeSubscriber { peerId, peersetId ->
                            logger.info("New consensus leader elected: $peerId")
                            consensusLeaders[peersetId] = peerId
                        },
                    )
                }
            val finalConsensusAction =
                SignalListener {
                    if (it.peerResolver.currentPeer() in counters) {
                        logger.info("Peer: ${it.peerResolver.currentPeer()} arrived")
                        finalConsensusPhaser.arrive()
                    } else {
                        counters.add(it.peerResolver.currentPeer())
                    }
                }

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2"),
                        "peerset1" to listOf("peer3", "peer4", "peer5"),
                    ),
                    signalListeners =
                        (0..2).map { "peer$it" }.associateWith {
                            mapOf(
                                Signal.ConsensusLeaderElected to leaderElectedListener,
                                Signal.TwoPCOnChangeApplied to
                                    SignalListener {
                                        askForDecisionPhaser.arrive()
                                        finalPhaser.arrive()
                                    },
                                Signal.ConsensusFollowerChangeAccepted to finalConsensusAction,
                            )
                        } +
                            (3..5).map { "peer$it" }.associateWith {
                                mapOf(
                                    Signal.ConsensusLeaderElected to leaderElectedListener,
                                    Signal.TwoPCOnHandleDecision to changeHandleDecisionListener,
                                    Signal.TwoPCOnAskForDecision to
                                        SignalListener {
                                            askedForDecision.set(true)
                                            runBlocking {
                                                logger.info("Waiting for ask for decision phaser")
                                                askForDecisionPhaser.arriveAndAwaitAdvanceWithTimeout()
                                            }
                                        },
                                    Signal.TwoPCOnHandleDecisionEnd to
                                        SignalListener {
                                            finalPhaser.arrive()
                                        },
                                    Signal.ConsensusFollowerChangeAccepted to finalConsensusAction,
                                )
                            },
                    subscribers =
                        (0..2).map { "peer$it" }.associateWith { mapOf(PeersetId("peerset0") to subscribers()) } +
                            (3..5).map { "peer$it" }.associateWith { mapOf(PeersetId("peerset1") to subscribers()) },
                    configOverrides = (3..5).map { "peer$it" }.associateWith { mapOf("twoPC.changeDelay" to Duration.ZERO) },
                )

            consensusLeaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()

            expectThat(consensusLeaders[PeersetId("peerset0")]).isNotNull()
            expectThat(consensusLeaders[PeersetId("peerset1")]).isNotNull()

            val firstConsensusLeader = consensusLeaders[PeersetId("peerset0")]!!

            expectCatching {
                val change1 = change(0, 1)
                executeChangeSync(firstConsensusLeader.toString(), "peerset0", change1, true)
            }.isSuccess()

            finalPhaser.arriveAndAwaitAdvanceWithTimeout()
            finalConsensusPhaser.arriveAndAwaitAdvanceWithTimeout()

            expectThat(askedForDecision.get()).isTrue()

            askAllForChanges("peerset0", "peerset1").forEach {
                logger.info("changes: $it")
                expectThat(it.size).isEqualTo(2)
                expectThat(it[1]).isA<StandardChange>()
            }
        }

    private suspend fun executeChange(
        uri: String,
        change: Change,
    ): HttpResponse =
        testHttpClient.post(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            setBody(change)
        }

    private suspend fun askForChanges(
        peer: PeerAddress,
        peersetId: String,
    ): Changes =
        testHttpClient.get("http://${peer.address}/changes?peerset=$peersetId") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.body()

    private suspend fun askAllForChanges(vararg peersetIds: String) =
        peersetIds.flatMap { peersetId ->
            apps.getPeerAddresses(peersetId).values.map { askForChanges(it, peersetId) }
        }

    private suspend fun askAllRunningPeersForChanges(vararg peersetIds: String) =
        peersetIds.flatMap { peersetId ->
            apps.getPeerAddresses(peersetId).values
                .filter { it.address != NON_RUNNING_PEER }
                .map { askForChanges(it, peersetId) }
        }

    private fun change(vararg peersetNums: Int) =
        StandardChange(
            "change",
            peersets =
                peersetNums.map {
                    ChangePeersetInfo(PeersetId("peerset$it"), InitialHistoryEntry.getId())
                },
        )

    private fun change(vararg peersetToChangeId: Pair<Int, String?>) =
        StandardChange(
            "change",
            peersets =
                peersetToChangeId.map {
                    ChangePeersetInfo(PeersetId("peerset${it.first}"), it.second)
                },
        )

    private fun twoPeersetChange(change: Change) =
        StandardChange(
            "change",
            peersets =
                (0..1).map { PeersetId("peerset$it") }
                    .map { ChangePeersetInfo(it, null) },
        )
}
