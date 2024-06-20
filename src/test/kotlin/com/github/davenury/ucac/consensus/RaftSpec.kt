package com.github.davenury.ucac.consensus

import com.github.davenury.common.Change
import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.Changes
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.common.StandardChange
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.common.history.PersistentHistory
import com.github.davenury.common.persistence.InMemoryPersistence
import com.github.davenury.ucac.ApplicationUcac
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.commitment.gpac.Accept
import com.github.davenury.ucac.commitment.gpac.Apply
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.structure.Subscribers
import com.github.davenury.ucac.consensus.raft.RaftConsensusProtocol
import com.github.davenury.ucac.consensus.raft.RaftConsensusProtocolImpl
import com.github.davenury.ucac.testHttpClient
import com.github.davenury.ucac.utils.IntegrationTestBase
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.davenury.ucac.utils.arriveAndAwaitAdvanceWithTimeout
import io.ktor.client.request.accept
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.statement.HttpResponse
import io.ktor.http.ContentType
import io.ktor.http.contentType
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.fail
import org.junitpioneer.jupiter.RetryingTest
import org.slf4j.LoggerFactory
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.assertions.isA
import strikt.assertions.isContainedIn
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isFalse
import strikt.assertions.isGreaterThanOrEqualTo
import strikt.assertions.isLessThanOrEqualTo
import strikt.assertions.isNotEqualTo
import strikt.assertions.isSuccess
import strikt.assertions.isTrue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.collections.Collection
import kotlin.collections.List
import kotlin.collections.MutableSet
import kotlin.collections.associateBy
import kotlin.collections.associateWith
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.drop
import kotlin.collections.filter
import kotlin.collections.first
import kotlin.collections.forEach
import kotlin.collections.forEachIndexed
import kotlin.collections.listOf
import kotlin.collections.map
import kotlin.collections.mapOf
import kotlin.collections.mapValues
import kotlin.collections.mutableSetOf
import kotlin.collections.plus
import kotlin.collections.set
import kotlin.collections.take
import kotlin.collections.toMutableMap
import kotlin.system.measureTimeMillis

// TODO: Add test in all consensuses that we run x changes and after finishing them, all peers has got consistent state

@ExtendWith(TestLogExtension::class)
class RaftSpec : IntegrationTestBase() {
    private val knownPeerIp = "localhost"
    private val unknownPeerIp = "198.18.0.0"
    private val noneLeader = null

    @BeforeEach
    fun setUp() {
        System.setProperty("configFile", "raft_application.conf")
    }

    @Test
    fun `happy path`(): Unit =
        runBlocking {
            val change1 = createChange()
            val change2 =
                createChange(
                    content = "change2",
                    parentId = change1.toHistoryEntry(PeersetId("peerset0")).getId(),
                )

            val peersWithoutLeader = 4

            val phaser = Phaser(peersWithoutLeader)
            phaser.register()

            val peerLeaderElected =
                SignalListener {
                    expectThat(phaser.phase).isEqualTo(0)
                    logger.info("Arrived leader elected ${it.subject.getPeerName()}")
                    phaser.arrive()
                }

            val peerApplyChange =
                SignalListener {
                    expectThat(phaser.phase).isContainedIn(listOf(1, 2))
                    logger.info("Arrived change ${it.subject.getPeerName()}")
                    phaser.arrive()
                }

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners =
                        (0..4).map { "peer$it" }.associateWith {
                            mapOf(
                                Signal.ConsensusLeaderElected to peerLeaderElected,
                                Signal.ConsensusFollowerChangeAccepted to peerApplyChange,
                            )
                        },
                )
            val peerAddresses = apps.getPeerAddresses("peerset0")

            phaser.arriveAndAwaitAdvanceWithTimeout()
            logger.info("Leader elected")

            // when: peer1 executed change
            expectCatching {
                executeChange("${apps.getPeer("peer0").address}/v2/change/sync?peerset=peerset0", change1)
            }.isSuccess()

            phaser.arriveAndAwaitAdvanceWithTimeout()
            logger.info("Change 1 applied")

            askAllForChanges(peerAddresses.values).forEach { changes ->
                // then: there's one change, and it's change we've requested
                expectThat(changes.size).isEqualTo(1)
                expect {
                    that(changes[0]).isEqualTo(change1)
                }
            }

            // when: peer2 executes change
            expectCatching {
                executeChange("${apps.getPeer("peer1").address}/v2/change/sync?peerset=peerset0", change2)
            }.isSuccess()

            phaser.arriveAndAwaitAdvanceWithTimeout()
            logger.info("Change 2 applied")

            askAllForChanges(peerAddresses.values).forEach { changes ->
                // then: there are two changes
                expectThat(changes.size).isEqualTo(2)
                expect {
                    that(changes[1]).isEqualTo(change2)
                    that(changes[0]).isEqualTo(change1)
                    that((changes[1] as StandardChange).content).isEqualTo(change2.content)
                }
            }
        }

    @Test
    fun `1000 change processed sequentially`(): Unit =
        runBlocking {
            val peersWithoutLeader = 4
            var iter = 0

            val leaderElectedPhaser = Phaser(peersWithoutLeader)
            leaderElectedPhaser.register()

            val phaser = Phaser(peersWithoutLeader)
            phaser.register()

            val peerLeaderElected =
                SignalListener {
                    if (leaderElectedPhaser.phase == 0) {
                        logger.info("Arrived ${it.subject.getPeerName()}")
                        leaderElectedPhaser.arrive()
                    }
                }

            val peerChangeAccepted =
                SignalListener {
                    logger.info("Arrived change$iter: ${it.change}")
                    phaser.arrive()
                }

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners =
                        (0..4).map { "peer$it" }.associateWith {
                            mapOf(
                                Signal.ConsensusLeaderElected to peerLeaderElected,
                                Signal.ConsensusFollowerChangeAccepted to peerChangeAccepted,
                            )
                        },
                )
            val peerAddresses = apps.getPeerAddresses("peerset0")

            leaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()
            logger.info("Leader elected")

            var change = createChange()

            val endRange = 1000

            var time = 0L

            repeat(endRange) {
                time +=
                    measureTimeMillis {
                        expectCatching {
                            executeChange("${apps.getPeer("peer0").address}/v2/change/sync?peerset=peerset0", change)
                        }.isSuccess()
                    }
                phaser.arriveAndAwaitAdvanceWithTimeout()
                iter += 1
                change = createChange(parentId = change.toHistoryEntry(PeersetId("peerset0")).getId())
            }
            // when: peer1 executed change

            expectThat(time / endRange).isLessThanOrEqualTo(500L)

            askAllForChanges(peerAddresses.values).forEach { changes ->
                // then: there are two changes
                expectThat(changes.size).isEqualTo(endRange)
            }
        }

    @RetryingTest(3)
    fun `process 50 changes, then one peer doesn't respond on 250 changes and finally synchronize on all`(): Unit =
        runBlocking {
            val peersWithoutLeader = 4
            var iter = 0
            val isFirstPartCommitted = AtomicBoolean(false)
            val isAllChangeCommitted = AtomicBoolean(false)
            var change = createChange()
            val firstPart = 100
            val secondPart = 400

            val leaderElectedPhaser = Phaser(peersWithoutLeader)
            val allPeerChangePhaser = Phaser(peersWithoutLeader)
            val changePhaser = Phaser(peersWithoutLeader - 1)
            val endingPhaser = Phaser(1)
            listOf(leaderElectedPhaser, allPeerChangePhaser, changePhaser, endingPhaser).forEach { it.register() }

            val peerLeaderElected =
                SignalListener {
                    if (leaderElectedPhaser.phase == 0) {
                        logger.info("Arrived ${it.subject.getPeerName()}")
                        leaderElectedPhaser.arrive()
                    }
                }

            val peerChangeAccepted =
                SignalListener {
                    logger.info("Arrived change: ${(it.change as StandardChange?)?.content}")
                    if (isFirstPartCommitted.get()) {
                        changePhaser.arrive()
                    } else {
                        allPeerChangePhaser.arrive()
                    }
                }

            val ignoringPeerChangeAccepted =
                SignalListener {
                    logger.info("Arrived change: ${(it.change as StandardChange?)?.content}")
                    if (isAllChangeCommitted.get() && (it.change as StandardChange?)?.content == "change${firstPart + secondPart - 1}") {
                        endingPhaser.arrive()
                    } else if (!isFirstPartCommitted.get()) {
                        allPeerChangePhaser.arrive()
                    }
                }

            val ignoreHeartbeat =
                SignalListener {
                    if (isFirstPartCommitted.get() && !isAllChangeCommitted.get()) {
                        throw RuntimeException(
                            "Ignore heartbeat",
                        )
                    }
                }

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners =
                        (0..3).map { "peer$it" }.associateWith {
                            mapOf(
                                Signal.ConsensusLeaderElected to peerLeaderElected,
                                Signal.ConsensusFollowerChangeAccepted to peerChangeAccepted,
                            )
                        } +
                            mapOf(
                                "peer4" to
                                    mapOf(
                                        Signal.ConsensusLeaderElected to peerLeaderElected,
                                        Signal.ConsensusFollowerChangeAccepted to ignoringPeerChangeAccepted,
                                        Signal.ConsensusFollowerHeartbeatReceived to ignoreHeartbeat,
                                        Signal.ConsensusTryToBecomeLeader to
                                            SignalListener {
                                                throw RuntimeException("Don't try to become a leader")
                                            },
                                    ),
                            ),
                )
            val peerAddresses = apps.getPeerAddresses("peerset0")

            leaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()
            logger.info("Leader elected")

            repeat(firstPart) {
                expectCatching {
                    executeChange("${apps.getPeer("peer0").address}/v2/change/sync?peerset=peerset0", change)
                }.isSuccess()
                allPeerChangePhaser.arriveAndAwaitAdvanceWithTimeout()
                iter += 1
                change = createChange(content = "change$it", parentId = change.toHistoryEntry(PeersetId("peerset0")).getId())
            }
            // when: peer1 executed change

            isFirstPartCommitted.set(true)

            repeat(secondPart) {
                expectCatching {
                    executeChange("${apps.getPeer("peer0").address}/v2/change/sync?peerset=peerset0", change)
                }.isSuccess()
                changePhaser.arriveAndAwaitAdvanceWithTimeout()
                iter += 1
                logger.info("Change second part moved $it")
                change = createChange(content = "change${it + 1 + firstPart}", parentId = change.toHistoryEntry(PeersetId("peerset0")).getId())
            }

            isAllChangeCommitted.set(true)

            endingPhaser.arriveAndAwaitAdvanceWithTimeout()

            askAllForChanges(peerAddresses.values).forEach { changes ->
                // then: there are two changes
                expectThat(changes.size).isEqualTo(firstPart + secondPart)
            }
        }

    @Test
    fun `change should be applied without waiting for election`(): Unit =
        runBlocking {
            val peersWithoutLeader = 4

            val phaser = Phaser(peersWithoutLeader)
            phaser.register()

            val peerApplyChange =
                SignalListener {
                    expectThat(phaser.phase).isEqualTo(0)
                    logger.info("Arrived ${it.subject.getPeerName()}")
                    phaser.arrive()
                }

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners =
                        (0..4).map { "peer$it" }.associateWith {
                            mapOf(
                                Signal.ConsensusFollowerChangeAccepted to peerApplyChange,
                            )
                        },
                )
            val peerAddresses = apps.getPeerAddresses("peerset0")

            logger.info("Sending change")

            val change = createChange()
            expectCatching {
                executeChange("${apps.getPeer("peer0").address}/v2/change/sync?peerset=peerset0", change)
            }.isSuccess()

            phaser.arriveAndAwaitAdvanceWithTimeout()
            logger.info("Change 1 applied")

            askAllForChanges(peerAddresses.values).forEach { changes ->
                expectThat(changes.size).isEqualTo(1)
                expect {
                    that(changes[0]).isEqualTo(change)
                }
            }
        }

    @Test
    fun `less than half of peers respond on ConsensusElectMe`(): Unit =
        runBlocking {
            val activePeers = 2
            val triesToBecomeLeader = 2
            val phaser = Phaser(activePeers * triesToBecomeLeader)

            val peerTryToBecomeLeader =
                SignalListener {
                    logger.info("Arrived ${it.subject.getPeerName()}")
                    phaser.arrive()
                }

            val signalListener =
                mapOf(
                    Signal.ConsensusTryToBecomeLeader to peerTryToBecomeLeader,
                )
            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    appsToExclude = listOf("peer2", "peer3", "peer4"),
                    signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
                )

            phaser.arriveAndAwaitAdvanceWithTimeout()

            apps.getRunningApps().forEach {
                expect {
                    val leaderAddress = askForLeaderAddress(it)
//              DONE  it should always be noneLeader
                    that(leaderAddress).isEqualTo(noneLeader)
                }
            }
        }

    @Test
    fun `minimum number of peers respond on ConsensusElectMe`(): Unit =
        runBlocking {
            val peersWithoutLeader = 3
            val phaser = Phaser(peersWithoutLeader)
            var isLeaderElected = false

            val peerLeaderElected =
                SignalListener {
                    if (!isLeaderElected) {
                        logger.info("Arrived ${it.subject.getPeerName()}")
                        phaser.arrive()
                    } else {
                        logger.debug("Leader is elected, not arriving")
                    }
                }

            val signalListener =
                mapOf(
                    Signal.ConsensusLeaderElected to peerLeaderElected,
                )

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    appsToExclude = listOf("peer3", "peer4"),
                    signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
                )

            phaser.arriveAndAwaitAdvanceWithTimeout()
            isLeaderElected = true

            apps.getRunningApps().forEach {
                expect {
                    val leaderAddress = askForLeaderAddress(it)
                    that(leaderAddress).isNotEqualTo(noneLeader)
                }
            }
        }

    @Test
    fun `leader failed and new leader is elected`(): Unit =
        runBlocking {
            val peersWithoutLeader = 4

            val election1Phaser = Phaser(peersWithoutLeader)
            val election2Phaser = Phaser(peersWithoutLeader - 1)
            listOf(election1Phaser, election2Phaser).forEach { it.register() }

            val peerLeaderElected =
                SignalListener {
                    if (election1Phaser.phase == 0) election1Phaser.arrive() else election2Phaser.arrive()
                }

            val signalListener = mapOf(Signal.ConsensusLeaderElected to peerLeaderElected)

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
                )
            var peers = apps.getRunningApps()

            election1Phaser.arriveAndAwaitAdvanceWithTimeout()

            val firstLeaderAddress = getLeaderAddress(peers[0])

            apps.getApp(firstLeaderAddress.peerId).stop(0, 0)

            peers = peers.filter { it.getPeerId() != firstLeaderAddress.peerId }

            election2Phaser.arriveAndAwaitAdvanceWithTimeout()

            expect {
                val secondLeaderAddress =
                    askForLeaderAddress(peers.first())
                that(secondLeaderAddress).isNotEqualTo(noneLeader)
                that(secondLeaderAddress).isNotEqualTo(firstLeaderAddress.address)
            }
        }

    @Test
    fun `less than half peers failed`(): Unit =
        runBlocking {
            val peersWithoutLeader = 3

            val election1Phaser = Phaser(peersWithoutLeader)
            val election2Phaser = Phaser(peersWithoutLeader - 1)
            listOf(election1Phaser, election2Phaser).forEach { it.register() }

            val peerLeaderElected =
                SignalListener {
                    if (election1Phaser.phase == 0) {
                        logger.info("Arrived at election 1 ${it.subject.getPeerName()}")
                        election1Phaser.arrive()
                    } else {
                        logger.info("Arrived at election 2 ${it.subject.getPeerName()}")
                        election2Phaser.arrive()
                    }
                }

            val signalListener = mapOf(Signal.ConsensusLeaderElected to peerLeaderElected)

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
                    appsToExclude = listOf("peer4"),
                )
            var peers = apps.getRunningApps()

            election1Phaser.arriveAndAwaitAdvanceWithTimeout()

            val firstLeaderAddress = getLeaderAddress(peers[0])

            apps.getApp(firstLeaderAddress.peerId).stop(0, 0)

            peers = peers.filter { it.getPeerId() != firstLeaderAddress.peerId }

            election2Phaser.arriveAndAwaitAdvanceWithTimeout()

            expect {
                val secondLeaderAddress =
                    askForLeaderAddress(peers.first())
                that(secondLeaderAddress).isNotEqualTo(noneLeader)
                that(secondLeaderAddress).isNotEqualTo(firstLeaderAddress.address)
            }
        }

    //    DONE: Exactly half of peers is running
    @Test
    fun `exactly half of peers is failed`(): Unit =
        runBlocking {
            val peersWithoutLeader = 3
            val activePeers = 3
            val peersTried: MutableSet<String> = mutableSetOf()
            var leaderElect = false

            val leaderFailedPhaser = Phaser(peersWithoutLeader)
            val electionPhaser = Phaser(peersWithoutLeader)
            val tryToBecomeLeaderPhaser = Phaser(activePeers)

            listOf(leaderFailedPhaser, electionPhaser, tryToBecomeLeaderPhaser).forEach { it.register() }

            val peerTryToBecomeLeader =
                SignalListener {
                    val name = it.subject.getPeerName()
                    if (!peersTried.contains(name) && leaderElect) {
                        peersTried.add(name)
                        logger.info("Arrived peerTryToBecomeLeader ${it.subject.getPeerName()}")
                        tryToBecomeLeaderPhaser.arrive()
                    }
                }

            val peerLeaderFailed =
                SignalListener {
                    logger.info("Arrived peerLeaderFailed ${it.subject.getPeerName()}")
                    leaderFailedPhaser.arrive()
                }
            val peerLeaderElected =
                SignalListener {
                    logger.info("Arrived peerLeaderElected ${it.subject.getPeerName()}")
                    electionPhaser.arrive()
                }

            val signalListener =
                mapOf(
                    Signal.ConsensusLeaderDoesNotSendHeartbeat to peerLeaderFailed,
                    Signal.ConsensusLeaderElected to peerLeaderElected,
                    Signal.ConsensusTryToBecomeLeader to peerTryToBecomeLeader,
                )

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4", "peer5"),
                    ),
                    appsToExclude = listOf("peer4", "peer5"),
                    signalListeners = (0..5).map { "peer$it" }.associateWith { signalListener },
                )
            var peers = apps.getRunningApps()

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            val firstLeaderAddress = getLeaderAddress(peers[0])

            apps.getApp(firstLeaderAddress.peerId).stop(0, 0)
            leaderElect = true

            peers = peers.filter { it.getPeerId() != firstLeaderAddress.peerId }

            leaderFailedPhaser.arriveAndAwaitAdvanceWithTimeout()
            tryToBecomeLeaderPhaser.arriveAndAwaitAdvanceWithTimeout()

            expect {
                val secondLeaderAddress = askForLeaderAddress(peers.first())
                that(secondLeaderAddress).isEqualTo(noneLeader)
            }
        }

    @Test
    fun `leader fails during processing change`(): Unit =
        runBlocking {
            val change = createChange()
            var peersWithoutLeader = 4

            val failurePhaser = Phaser(2)
            val election1Phaser = Phaser(peersWithoutLeader)
            peersWithoutLeader -= 1
            val election2Phaser = Phaser(peersWithoutLeader)
            val changePhaser = Phaser(3)
            listOf(election1Phaser, election2Phaser, changePhaser).forEach { it.register() }
            var firstLeader = true
            val proposedPeers = ConcurrentHashMap<String, Boolean>()
            val mutex = Mutex()

            val leaderAction =
                SignalListener {
                    if (firstLeader) {
                        logger.info("Arrived leader action ${it.subject.getPeerName()}")
                        failurePhaser.arrive()
                        throw RuntimeException("Failed after proposing change")
                    }
                }

            val peerLeaderElected =
                SignalListener {
                    when (election1Phaser.phase) {
                        0 -> {
                            logger.info("Arrived at election 1 ${it.subject.getPeerName()}")
                            election1Phaser.arrive()
                        }

                        else -> {
                            logger.info("Arrived at election 2 ${it.subject.getPeerName()}")
                            election2Phaser.arrive()
                        }
                    }
                }

            val peerApplyChange =
                SignalListener {
                    logger.info("Arrived peer apply change")
                    changePhaser.arrive()
                }
            val ignoreHeartbeatAfterProposingChange =
                SignalListener {
                    runBlocking {
                        mutex.withLock {
                            when {
                                it.change == change && firstLeader && !proposedPeers.contains(it.subject.getPeerName()) -> {
                                    proposedPeers[it.subject.getPeerName()] = true
                                }
                                proposedPeers.contains(it.subject.getPeerName()) && firstLeader -> throw Exception(
                                    "Ignore heartbeat from old leader",
                                )
                                proposedPeers.size > 2 && firstLeader -> throw Exception(
                                    "Ignore heartbeat from old leader",
                                )
                            }
                        }
                    }
                }

            val leaderSendingHeartbeat =
                SignalListener {
                    runBlocking {
                        mutex.withLock {
                            if (firstLeader && proposedPeers.size > 2) {
                                throw RuntimeException(
                                    "Stop handling heartbeats from old leader",
                                )
                            }
                        }
                    }
                }

            val signalListener =
                mapOf(
                    Signal.ConsensusAfterProposingChange to leaderAction,
                    Signal.ConsensusLeaderElected to peerLeaderElected,
                    Signal.ConsensusFollowerChangeAccepted to peerApplyChange,
                    Signal.ConsensusFollowerHeartbeatReceived to ignoreHeartbeatAfterProposingChange,
                    Signal.ConsensusFollowerHeartbeatReceived to leaderSendingHeartbeat,
                )

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
                )

            election1Phaser.arriveAndAwaitAdvanceWithTimeout()

            val firstLeaderAddress = getLeaderAddress(apps.getRunningApps()[0])

//      Start processing
            expectCatching {
                executeChange("${firstLeaderAddress.address}/v2/change/sync?peerset=peerset0&timeout=PT0.5S", change)
            }.isFailure()

            failurePhaser.arriveAndAwaitAdvanceWithTimeout()

            apps.getApp(firstLeaderAddress.peerId).stop(0, 0)

            election2Phaser.arriveAndAwaitAdvanceWithTimeout()

            firstLeader = false

            changePhaser.arriveAndAwaitAdvanceWithTimeout()

            apps.getRunningPeers("peerset0")
                .values
                .filter { it != firstLeaderAddress }
                .forEach {
                    val proposedChanges2 = askForProposedChanges(it)
                    val acceptedChanges2 = askForAcceptedChanges(it)
                    expect {
                        that(proposedChanges2.size).isEqualTo(0)
                        that(acceptedChanges2.size).isEqualTo(1)
                    }
                    expect {
                        that(acceptedChanges2.first()).isEqualTo(change)
                    }
                }
        }

    @Test
    fun `less than half of peers fails after electing leader`(): Unit =
        runBlocking {
            val peersWithoutLeader = 4

            val electionPhaser = Phaser(peersWithoutLeader)
            val changePhaser = Phaser(peersWithoutLeader - 2)
            listOf(electionPhaser, changePhaser).forEach { it.register() }

            val signalListener =
                mapOf(
                    Signal.ConsensusLeaderElected to
                        SignalListener {
                            logger.info("Arrived at election ${it.subject.getPeerName()}")
                            electionPhaser.arrive()
                        },
                    Signal.ConsensusFollowerChangeAccepted to
                        SignalListener {
                            logger.info("Arrived at apply ${it.subject.getPeerName()}")
                            changePhaser.arrive()
                        },
                )

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
                )
            val peers = apps.getRunningApps()

            val peerAddresses = apps.getRunningPeers("peerset0").values

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            val firstLeaderAddress = getLeaderAddress(peers[0])

            val peersToStop = peerAddresses.filter { it != firstLeaderAddress }.take(2)
            peersToStop.forEach { apps.getApp(it.peerId).stop(0, 0) }
            val runningPeers = peerAddresses.filter { address -> address !in peersToStop }
            val change = createChange()

//      Start processing
            expectCatching {
                executeChange("${runningPeers.first().address}/v2/change/sync?peerset=peerset0", change)
            }.isSuccess()

            changePhaser.arriveAndAwaitAdvanceWithTimeout()

            runningPeers.forEach {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                expect {
                    that(proposedChanges.size).isEqualTo(0)
                    that(acceptedChanges.size).isEqualTo(1)
                }
                expect {
                    that(acceptedChanges.first()).isEqualTo(change)
                }
            }
        }

    @Test
    fun `more than half of peers fails during propagating change`(): Unit =
        runBlocking {
            val peersWithoutLeader = 4

            val electionPhaser = Phaser(peersWithoutLeader)
            val changePhaser = Phaser(peersWithoutLeader - 3)
            listOf(electionPhaser, changePhaser).forEach { it.register() }

            val peerLeaderElected = SignalListener { electionPhaser.arrive() }
            val peerApplyChange =
                SignalListener {
                    logger.info("Arrived ${it.subject.getPeerName()}")
                    changePhaser.arrive()
                }

            val signalListener =
                mapOf(
                    Signal.ConsensusLeaderElected to peerLeaderElected,
                    Signal.ConsensusFollowerChangeProposed to peerApplyChange,
                )

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
                )
            val peers = apps.getRunningApps()

            val peerAddresses = apps.getRunningPeers("peerset0").values

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            val firstLeaderAddress = getLeaderAddress(peers[0])

            val peersToStop = peerAddresses.filter { it != firstLeaderAddress }.take(3)
            peersToStop.forEach { apps.getApp(it.peerId).stop(0, 0) }
            val runningPeers = peerAddresses.filter { address -> address !in peersToStop }
            val change = createChange()

//      Start processing
            expectCatching {
                executeChange("${runningPeers.first().address}/v2/change/async?peerset=peerset0", change)
            }.isSuccess()

            changePhaser.arriveAndAwaitAdvanceWithTimeout()

//      As only one peer confirm changes it should be still proposedChange
            runningPeers.forEach {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                expect {
                    that(proposedChanges.size).isEqualTo(1)
                    that(acceptedChanges.size).isEqualTo(0)
                }
                expect {
                    that(proposedChanges.first()).isEqualTo(change)
                }
            }
        }

    @Test
    fun `network divide on half and then merge`(): Unit =
        runBlocking {
            var peersWithoutLeader = 4

            var isNetworkDivided = false

            val election1Phaser = Phaser(peersWithoutLeader)
            peersWithoutLeader -= 2
            val election2Phaser = Phaser(peersWithoutLeader)
            val change1Phaser = Phaser(peersWithoutLeader)
            val change2Phaser = Phaser(peersWithoutLeader)
            listOf(election1Phaser, election2Phaser, change1Phaser, change2Phaser).forEach { it.register() }

            val signalListener =
                mapOf(
                    Signal.ConsensusLeaderElected to
                        SignalListener {
                            when {
                                election1Phaser.phase == 0 -> {
                                    logger.info("Arrived at election 1 ${it.subject.getPeerName()}")
                                    election1Phaser.arrive()
                                }

                                isNetworkDivided && election2Phaser.phase == 0 -> {
                                    logger.info("Arrived at election 2 ${it.subject.getPeerName()}")
                                    election2Phaser.arrive()
                                }
                            }
                        },
                    Signal.ConsensusFollowerChangeAccepted to
                        SignalListener {
                            if (change1Phaser.phase == 0) {
                                logger.info("Arrived at change 1 ${it.subject.getPeerName()}")
                                change1Phaser.arrive()
                            } else {
                                logger.info("Arrived at change 2 ${it.subject.getPeerName()}")
                                change2Phaser.arrive()
                            }
                        },
                )

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
                )
            val peers = apps.getRunningApps()

            val peerAddresses = apps.getRunningPeers("peerset0").values
            val peerAddresses2 = apps.getRunningPeers("peerset0")

            election1Phaser.arriveAndAwaitAdvanceWithTimeout()

            logger.info("First election finished")

            val firstLeaderAddress = getLeaderAddress(peers[0])

            logger.info("First leader: $firstLeaderAddress")

            val notLeaderPeers = peerAddresses.filter { it != firstLeaderAddress }

            val firstHalf: List<PeerAddress> = listOf(firstLeaderAddress, notLeaderPeers.first())
            val secondHalf: List<PeerAddress> = notLeaderPeers.drop(1)

//      Divide network
            isNetworkDivided = true

            firstHalf.forEach { address ->
                val peerAddresses =
                    apps.getRunningPeers("peerset0").mapValues { entry ->
                        val peer = entry.value
                        if (secondHalf.contains(peer)) {
                            peer.copy(address = peer.address.replace(knownPeerIp, unknownPeerIp))
                        } else {
                            peer
                        }
                    }
                apps.getApp(address.peerId).setPeerAddresses(peerAddresses)
            }

            secondHalf.forEach { address ->
                val peerAddresses =
                    apps.getRunningPeers("peerset0").mapValues { entry ->
                        val peer = entry.value
                        if (firstHalf.contains(peer)) {
                            peer.copy(address = peer.address.replace(knownPeerIp, unknownPeerIp))
                        } else {
                            peer
                        }
                    }
                apps.getApp(address.peerId).setPeerAddresses(peerAddresses)
            }

            logger.info("Network divided")

            election2Phaser.arriveAndAwaitAdvanceWithTimeout()

            logger.info("Second election finished")

//      Check if second half chose new leader
            secondHalf.forEachIndexed { index, peer ->
                val app = apps.getApp(peer.peerId)
                val newLeaderAddress = askForLeaderAddress(app)
                expectThat(newLeaderAddress).isNotEqualTo(firstLeaderAddress.address)

                // log only once
                if (index == 0) {
                    logger.info("New leader: $newLeaderAddress")
                }
            }

            val change1 = createChange(content = "change1")
            val change2 = createChange(content = "change2")

//      Run change in both halfs
            expectCatching {
                executeChange("${firstHalf.first().address}/v2/change/async?peerset=peerset0", change1)
            }.isSuccess()

            expectCatching {
                executeChange("${secondHalf.first().address}/v2/change/async?peerset=peerset0", change2)
            }.isSuccess()

            change1Phaser.arriveAndAwaitAdvanceWithTimeout()

            logger.info("After change 1")

            firstHalf.forEach {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                logger.debug("Checking $it proposed: $proposedChanges accepted: $acceptedChanges")
                expect {
                    that(proposedChanges.size).isEqualTo(1)
                    that(acceptedChanges.size).isEqualTo(0)
                }
                expect {
                    that(proposedChanges.first()).isEqualTo(change1)
                    that((proposedChanges.first() as StandardChange).content).isEqualTo(change1.content)
                }
            }

            secondHalf.forEach {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                logger.debug("Checking $it proposed: $proposedChanges accepted: $acceptedChanges")
                expect {
                    that(proposedChanges.size).isEqualTo(0)
                    that(acceptedChanges.size).isEqualTo(1)
                }
                expect {
                    that(acceptedChanges.first()).isEqualTo(change2)
                    that((acceptedChanges.first() as StandardChange).content).isEqualTo(change2.content)
                }
            }

//      Merge network
            peerAddresses.forEach { address ->
                apps.getApp(address.peerId).setPeerAddresses(peerAddresses2)
            }

            logger.info("Network merged")

            change2Phaser.arriveAndAwaitAdvanceWithTimeout()

            logger.info("After change 2")

            peerAddresses.forEach {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                logger.debug("Checking $it proposed: $proposedChanges accepted: $acceptedChanges")
                expect {
                    that(proposedChanges.size).isEqualTo(0)
                    that(acceptedChanges.size).isEqualTo(1)
                }
                expect {
                    that(acceptedChanges.first()).isEqualTo(change2)
                    that((acceptedChanges.first() as StandardChange).content).isEqualTo(change2.content)
                }
            }
        }

    @Test
    fun `unit tests of isMoreThanHalf() function`(): Unit =
        runBlocking {
            val peers =
                listOf(
                    PeerAddress(PeerId("peer0"), "1"),
                    PeerAddress(PeerId("peer1"), "2"),
                    PeerAddress(PeerId("peer2"), "3"),
                    PeerAddress(PeerId("peer3"), "4"),
                    PeerAddress(PeerId("peer4"), "5"),
                    PeerAddress(PeerId("peer5"), "6"),
                )
                    .associateBy { it.peerId }
                    .toMutableMap()

            val peerResolver =
                PeerResolver(
                    PeerId("peer0"),
                    peers,
                    mapOf(
                        PeersetId("peerset0") to listOf(PeerId("peer0")),
                    ),
                )
            val consensus =
                RaftConsensusProtocolImpl(
                    PeersetId("peerset0"),
                    PersistentHistory(InMemoryPersistence()),
                    mockk(),
                    peerResolver,
                    protocolClient = mockk(),
                    transactionBlocker = mockk(),
                    isMetricTest = false,
                    maxChangesPerMessage = 200,
                    subscribers = Subscribers(),
                )
            expect {
                that(consensus.isMoreThanHalf(0)).isTrue()
            }

            peerResolver.addPeerToPeerset(
                PeersetId("peerset0"),
                PeerId("peer1"),
            )
            expect {
                that(consensus.isMoreThanHalf(0)).isFalse()
                that(consensus.isMoreThanHalf(1)).isTrue()
            }

            peerResolver.addPeerToPeerset(
                PeersetId("peerset0"),
                PeerId("peer2"),
            )
            expect {
                that(consensus.isMoreThanHalf(0)).isFalse()
                that(consensus.isMoreThanHalf(1)).isTrue()
                that(consensus.isMoreThanHalf(2)).isTrue()
            }

            peerResolver.addPeerToPeerset(
                PeersetId("peerset0"),
                PeerId("peer3"),
            )
            expect {
                that(consensus.isMoreThanHalf(0)).isFalse()
                that(consensus.isMoreThanHalf(1)).isFalse()
                that(consensus.isMoreThanHalf(2)).isTrue()
                that(consensus.isMoreThanHalf(3)).isTrue()
            }

            peerResolver.addPeerToPeerset(
                PeersetId("peerset0"),
                PeerId("peer4"),
            )
            expect {
                that(consensus.isMoreThanHalf(0)).isFalse()
                that(consensus.isMoreThanHalf(1)).isFalse()
                that(consensus.isMoreThanHalf(2)).isTrue()
                that(consensus.isMoreThanHalf(3)).isTrue()
                that(consensus.isMoreThanHalf(4)).isTrue()
            }

            peerResolver.addPeerToPeerset(
                PeersetId("peerset0"),
                PeerId("peer4"),
            )
            expect {
                that(consensus.isMoreThanHalf(0)).isFalse()
                that(consensus.isMoreThanHalf(1)).isFalse()
                that(consensus.isMoreThanHalf(2)).isFalse()
                that(consensus.isMoreThanHalf(3)).isTrue()
                that(consensus.isMoreThanHalf(4)).isTrue()
                that(consensus.isMoreThanHalf(5)).isTrue()
            }
        }

    @Test
    fun `should synchronize on history if it was added outside of raft`(): Unit =
        runBlocking {
            val phaserGPACPeer = Phaser(1)
            val phaserRaftPeers = Phaser(4)
            val leaderElectedPhaser = Phaser(4)

            val isSecondGPAC = AtomicBoolean(false)

            listOf(phaserGPACPeer, phaserRaftPeers, leaderElectedPhaser).forEach { it.register() }

            val proposedChange =
                StandardChange(
                    "change",
                    peersets =
                        listOf(
                            ChangePeersetInfo(PeersetId("peerset0"), InitialHistoryEntry.getId()),
                        ),
                )

            val firstLeaderAction =
                SignalListener { signalData ->
                    val url = "http://${signalData.peerResolver.resolve("peer1").address}/protocols/gpac/apply?peerset=peerset0"
                    runBlocking {
                        testHttpClient.post<HttpResponse>(url) {
                            contentType(ContentType.Application.Json)
                            accept(ContentType.Application.Json)
                            body =
                                Apply(
                                    signalData.transaction!!.ballotNumber,
                                    true,
                                    Accept.COMMIT,
                                    signalData.change!!,
                                )
                        }.also {
                            logger.info("Got response ${it.status.value}")
                        }
                    }
                    throw RuntimeException("Stop leader after apply")
                }

            val peerGPACAction =
                SignalListener {
                    phaserGPACPeer.arrive()
                }
            val raftPeersAction =
                SignalListener {
                    phaserRaftPeers.arrive()
                }
            val leaderElectedAction = SignalListener { leaderElectedPhaser.arrive() }

            val firstPeerSignals =
                mapOf(
                    Signal.BeforeSendingApply to firstLeaderAction,
                    Signal.ConsensusFollowerChangeAccepted to raftPeersAction,
                    Signal.ConsensusLeaderElected to leaderElectedAction,
                    Signal.OnHandlingElectBegin to
                        SignalListener {
                            if (isSecondGPAC.get()) {
                                throw Exception("Ignore restarting GPAC")
                            }
                        },
                )

            val peerSignals =
                mapOf(
                    Signal.ConsensusLeaderElected to leaderElectedAction,
                    Signal.ConsensusFollowerChangeAccepted to raftPeersAction,
                    Signal.OnHandlingElectBegin to SignalListener { if (isSecondGPAC.get()) throw Exception("Ignore restarting GPAC") },
                )

            val peerRaftSignals =
                mapOf(
                    Signal.ConsensusLeaderElected to leaderElectedAction,
                    Signal.ConsensusFollowerChangeAccepted to raftPeersAction,
                    Signal.OnHandlingElectBegin to SignalListener { if (isSecondGPAC.get()) throw Exception("Ignore restarting GPAC") },
                    Signal.OnHandlingAgreeBegin to SignalListener { throw Exception("Ignore GPAC") },
                )

            val peer1Signals =
                mapOf(
                    Signal.OnHandlingApplyCommitted to peerGPACAction,
                    Signal.ConsensusLeaderElected to leaderElectedAction,
                    Signal.OnHandlingElectBegin to SignalListener { if (isSecondGPAC.get()) throw Exception("Ignore restarting GPAC") },
                )

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners =
                        mapOf(
                            "peer0" to firstPeerSignals,
                            "peer1" to peer1Signals,
                            "peer2" to peerSignals,
                            "peer3" to peerRaftSignals,
                            "peer4" to peerRaftSignals,
                        ),
                    configOverrides =
                        mapOf(
                            "peer0" to mapOf("gpac.maxLeaderElectionTries" to 2),
                            "peer1" to mapOf("gpac.maxLeaderElectionTries" to 2),
                            "peer2" to mapOf("gpac.maxLeaderElectionTries" to 2),
                            "peer3" to mapOf("gpac.maxLeaderElectionTries" to 2),
                            "peer4" to mapOf("gpac.maxLeaderElectionTries" to 2),
                        ),
                )

            leaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()

            // change that will cause leader to fall according to action
            try {
                executeChange(
                    "${apps.getPeer("peer0").address}/v2/change/sync?peerset=peerset0&enforce_gpac=true",
                    proposedChange,
                )
                fail("Change passed")
            } catch (e: Exception) {
                logger.info("Leader 1 fails", e)
            }

            // leader timeout is 5 seconds for integration tests - in the meantime other peer should wake up and execute transaction
            phaserGPACPeer.arriveAndAwaitAdvanceWithTimeout()
            isSecondGPAC.set(true)

            val change =
                testHttpClient.get<Change>("http://${apps.getPeer("peer1").address}/change?peerset=peerset0") {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                }

            expect {
                that(change).isA<StandardChange>()
                that((change as StandardChange).content).isEqualTo(proposedChange.content)
            }

            phaserRaftPeers.arriveAndAwaitAdvanceWithTimeout()

            apps.getPeerAddresses("peerset0").forEach { (_, peerAddress) ->
                // and should not execute this change couple of times
                val changes =
                    testHttpClient.get<Changes>("http://${peerAddress.address}/changes?peerset=peerset0") {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                    }

                // only one change and this change shouldn't be applied two times
                expectThat(changes.size).isGreaterThanOrEqualTo(1)
                expect {
                    that(changes[0]).isA<StandardChange>()
                    that((changes[0] as StandardChange).content).isEqualTo(proposedChange.content)
                }
            }
        }

    @Test
    fun `one peer consensus`(): Unit =
        runBlocking {
            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0"),
                    ),
                )

            expectCatching {
                val change1 = createChange()
                val peerAddress = apps.getPeer("peer0").address
                val change2 = createChange(parentId = change1.toHistoryEntry(PeersetId("peerset0")).getId())
                executeChange("$peerAddress/v2/change/sync?peerset=peerset0", change1)
                executeChange("$peerAddress/v2/change/sync?peerset=peerset0", change2)
            }.isSuccess()

            askAllForChanges(apps.getPeerAddresses("peerset0").values).forEach { changes ->
                expectThat(changes.size).isEqualTo(2)
            }
        }

    @Test
    fun `consensus on multiple peersets`(): Unit =
        runBlocking {
            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                        "peerset1" to listOf("peer1", "peer2", "peer4"),
                        "peerset2" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                        "peerset3" to listOf("peer2", "peer3"),
                    ),
                )

            val peersetCount = apps.getPeersets().size
            repeat(peersetCount) { i ->
                logger.info("Sending changes to peerset$i")

                val change1 = createChange(peersetId = "peerset$i")
                val parentId = change1.toHistoryEntry(PeersetId("peerset$i")).getId()
                val change2 = createChange(peersetId = "peerset$i", parentId = parentId)

                val peerAddress = apps.getPeerAddresses("peerset$i").values.iterator().next().address

                expectCatching {
                    executeChange("$peerAddress/v2/change/sync?peerset=peerset$i", change1)
                    executeChange("$peerAddress/v2/change/sync?peerset=peerset$i", change2)
                }.isSuccess()
            }

            repeat(peersetCount) { i ->
                val peerAddress = apps.getPeerAddresses("peerset$i").values.iterator().next()

                val changes = askForChanges(peerAddress)
                expectThat(changes.size).isEqualTo(2)
            }
        }

    private fun createChange(
        content: String = "change",
        parentId: String = InitialHistoryEntry.getId(),
        peersetId: String = "peerset0",
    ) = StandardChange(
        content,
        peersets = listOf(ChangePeersetInfo(PeersetId(peersetId), parentId)),
    )

    private suspend fun executeChange(
        uri: String,
        change: Change,
    ) = testHttpClient.post<String>("http://$uri") {
        contentType(ContentType.Application.Json)
        accept(ContentType.Application.Json)
        body = change
    }

    private suspend fun genericAskForChange(
        suffix: String,
        peerAddress: PeerAddress,
    ) = testHttpClient.get<Changes>("http://${peerAddress.address}/protocols/raft/$suffix?peerset=peerset0") {
        contentType(ContentType.Application.Json)
        accept(ContentType.Application.Json)
    }

    private suspend fun askForChanges(peerAddress: PeerAddress) =
        testHttpClient.get<Changes>("http://${peerAddress.address}/v2/change?peerset=peerset0") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

    private suspend fun askAllForChanges(peerAddresses: Collection<PeerAddress>) =
        peerAddresses.map {
            askForChanges(
                it,
            )
        }

    private suspend fun askForProposedChanges(peerAddress: PeerAddress) =
        genericAskForChange(
            "proposed_changes",
            peerAddress,
        )

    private suspend fun askForAcceptedChanges(peerAddress: PeerAddress) =
        genericAskForChange(
            "accepted_changes",
            peerAddress,
        )

    private fun askForLeaderAddress(app: ApplicationUcac): String? {
        val leaderId =
            (
                app.getPeersetProtocols(
                    PeersetId("peerset0"),
                ).consensusProtocol as RaftConsensusProtocol
            ).getLeaderId()
        return leaderId?.let { apps.getPeer(it).address }
    }

    private fun getLeaderAddress(app: ApplicationUcac): PeerAddress {
        val leaderId =
            (
                app.getPeersetProtocols(
                    PeersetId("peerset0"),
                ).consensusProtocol as RaftConsensusProtocol
            ).getLeaderId()!!
        return apps.getPeer(leaderId)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RaftSpec::class.java)
    }
}
