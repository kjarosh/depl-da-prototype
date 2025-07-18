package com.github.davenury.ucac.consensus

import com.github.davenury.common.Change
import com.github.davenury.common.ChangePeersetInfo
import com.github.davenury.common.Changes
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.common.StandardChange
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.ucac.ApplicationUcac
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.commitment.gpac.Accept
import com.github.davenury.ucac.commitment.gpac.Apply
import com.github.davenury.ucac.consensus.paxos.PaxosProtocol
import com.github.davenury.ucac.testHttpClient
import com.github.davenury.ucac.utils.IntegrationTestBase
import com.github.davenury.ucac.utils.TestApplicationSet
import com.github.davenury.ucac.utils.TestLogExtension
import com.github.davenury.ucac.utils.arriveAndAwaitAdvanceWithTimeout
import io.ktor.client.call.body
import io.ktor.client.request.accept
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.http.ContentType
import io.ktor.http.contentType
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
import strikt.assertions.isA
import strikt.assertions.isEqualTo
import strikt.assertions.isFailure
import strikt.assertions.isGreaterThanOrEqualTo
import strikt.assertions.isLessThanOrEqualTo
import strikt.assertions.isNotEqualTo
import strikt.assertions.isTrue
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.collections.Collection
import kotlin.collections.List
import kotlin.collections.any
import kotlin.collections.associateWith
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.drop
import kotlin.collections.eachCount
import kotlin.collections.filter
import kotlin.collections.first
import kotlin.collections.firstNotNullOf
import kotlin.collections.forEach
import kotlin.collections.groupingBy
import kotlin.collections.listOf
import kotlin.collections.map
import kotlin.collections.mapOf
import kotlin.collections.mapValues
import kotlin.collections.plus
import kotlin.collections.set
import kotlin.collections.take
import kotlin.collections.toList
import kotlin.system.measureTimeMillis

@ExtendWith(TestLogExtension::class)
class PaxosSpec : IntegrationTestBase() {
    private val knownPeerIp = "localhost"
    private val unknownPeerIp = "198.18.0.0"
    private val noneLeader = null

    @BeforeEach
    fun setUp() {
        System.setProperty("configFile", "paxos_application.conf")
    }

    @Test
    fun `happy path`(): Unit =
        runBlocking {
            val peersWithoutLeader = 5

            val leaderElectionPhaser = Phaser(1)
            val changePhaser = Phaser(peersWithoutLeader)
            listOf(leaderElectionPhaser, changePhaser).forEach { it.register() }

            val peerLeaderElected =
                SignalListener {
                    logger.info("Arrived leader election ${it.subject.getPeerName()}")
                    leaderElectionPhaser.arrive()
                }

            val peerApplyChange =
                SignalListener {
                    logger.info("Arrived ${it.subject.getPeerName()}")
                    changePhaser.arrive()
                }

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners =
                        (0..4).map { peer(it) }.associateWith {
                            mapOf(
                                Signal.PigPaxosLeaderElected to peerLeaderElected,
                                Signal.PigPaxosChangeCommitted to peerApplyChange,
                            )
                        },
                )
            val peerAddresses = apps.getRunningPeers("peerset0")

            leaderElectionPhaser.arriveAndAwaitAdvanceWithTimeout()
            logger.info("Leader elected")

            // when: peer1 executed change
            val change1 = createChange()
            executeChangeSync(peer(0), "peerset0", change1)

            changePhaser.arriveAndAwaitAdvanceWithTimeout()
            logger.info("Change 1 applied")

            askAllForChanges(peerAddresses.values).forEach { changes ->
                // then: there's one change, and it's change we've requested
                expectThat(changes.size).isEqualTo(1)
                expect {
                    that(changes[0]).isEqualTo(change1)
                }
            }

            // when: peer2 executes change
            val change2 = createChange(content = "change2", parentId = change1.toHistoryEntry(peerset()).getId())
            executeChangeSync(peer(1), "peerset0", change2)

            changePhaser.arriveAndAwaitAdvanceWithTimeout()
            logger.info("Change 2 applied")

            askAllForChanges(peerAddresses.values).forEach { changes ->
                // then: there are two changes
                expectThat(changes.size).isEqualTo(2)
                expect {
                    that(changes[1]).isEqualTo(change2)
                    that(changes[0]).isEqualTo(change1)
                    that((changes[1] as StandardChange).content).isEqualTo("change2")
                }
            }
        }

    @Test
    fun `1000 change processed sequentially`(): Unit =
        runBlocking {
            val peers = 5

            val leaderElectedPhaser = Phaser(1)
            leaderElectedPhaser.register()

            val phaser = Phaser(peers)
            phaser.register()

            var change = createChange()

            val peerLeaderElected =
                SignalListener {
                    logger.info("Arrived peer elected ${it.subject.getPeerName()}")
                    leaderElectedPhaser.arrive()
                }

            val peerChangeAccepted =
                SignalListener {
                    logger.info("Arrived change: ${it.change}")
                    if (it.change == change) phaser.arrive()
                }

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners =
                        (0..4).map { "peer$it" }.associateWith {
                            mapOf(
                                Signal.PigPaxosLeaderElected to peerLeaderElected,
                                Signal.PigPaxosChangeCommitted to peerChangeAccepted,
                            )
                        },
                )
            val peerAddresses = apps.getRunningPeers("peerset0")

            leaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()
            logger.info("Leader elected")

            val endRange = 1000

            var time = 0L

            (0 until endRange).forEach {
                val newTime =
                    measureTimeMillis {
                        executeChangeSync(peer(0), "peerset0", change)
                        phaser.arriveAndAwaitAdvanceWithTimeout(Duration.ofSeconds(30))
                    }
                logger.info("Change $it is processed $newTime ms")
                time += newTime

                change = createChange(parentId = change.toHistoryEntry(peerset()).getId())
            }
            // when: peer1 executed change

            expectThat(time / endRange).isLessThanOrEqualTo(500L)

            askAllForChanges(peerAddresses.values).forEach { changes ->
                // then: there are two changes
                expectThat(changes.size).isEqualTo(endRange)
            }
        }

    @Test
    fun `change should be applied without waiting for election`(): Unit =
        runBlocking {
            val peers = 5

            val phaser = Phaser(peers)
            phaser.register()

            val peerApplyChange =
                SignalListener {
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
                                Signal.PigPaxosChangeCommitted to peerApplyChange,
                            )
                        },
                )
            val peerAddresses = apps.getRunningPeers("peerset0")

            logger.info("Sending change")

            val change = createChange()
            executeChangeSync(peer(0), "peerset0", change)

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
                    Signal.PigPaxosTryToBecomeLeader to peerTryToBecomeLeader,
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

            val leaderNotElected =
                apps.getRunningApps().any {
                    askForLeaderAddress(it) == noneLeader
//              DONE  it should always be noneLeader
                }

            expectThat(leaderNotElected).isTrue()
        }

    @Test
    fun `minimum number of peers respond on ConsensusElectMe`(): Unit =
        runBlocking {
            val peersWithoutLeader = 2
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
                    Signal.PigPaxosLeaderElected to peerLeaderElected,
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

    @RetryingTest(5)
    fun `leader fails during processing change`(): Unit =
        runBlocking {
            val change = createChange()
            var peers = 5

            val failurePhaser = Phaser(2)
            val election1Phaser = Phaser(1)
            val election2Phaser = Phaser(1)
            peers -= 1
            val changePhaser = Phaser(4)
            listOf(election1Phaser, election2Phaser, changePhaser).forEach { it.register() }
            var firstLeader = true
            val proposedPeers = ConcurrentHashMap<String, Boolean>()
            var changePeers: (() -> Unit?)? = null

            val leaderAction =
                SignalListener {
                    if (firstLeader) {
                        logger.info("Arrived ${it.subject.getPeerName()}")
                        changePeers?.invoke()
                        failurePhaser.arrive()
                        throw RuntimeException("Failed after proposing change")
                    }
                }

            val peerLeaderElected =
                SignalListener {
                    when (failurePhaser.phase) {
                        0 -> {
                            logger.info("Arrived at election 1 ${it.subject.getPeerName()}")
                            election1Phaser.arrive()
                        }

                        else -> {
                            logger.info("Arrived at election 2 ${it.subject.getPeerName()}")
                            firstLeader = false
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
                    when {
                        it.change == change && firstLeader && !proposedPeers.contains(it.subject.getPeerName()) -> {
                            proposedPeers[it.subject.getPeerName()] = true
                        }

                        proposedPeers.contains(it.subject.getPeerName()) && firstLeader -> throw Exception(
                            "Ignore heartbeat from old leader",
                        )
                        proposedPeers.size > 2 && firstLeader -> throw Exception("Ignore heartbeat from old leader")
                    }
                }

            val signalListener =
                mapOf(
                    Signal.PigPaxosAfterAcceptChange to leaderAction,
                    Signal.PigPaxosLeaderElected to peerLeaderElected,
                    Signal.PigPaxosChangeCommitted to peerApplyChange,
                    Signal.PigPaxosReceivedCommit to ignoreHeartbeatAfterProposingChange,
                )

            apps =
                TestApplicationSet(
                    mapOf("peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4")),
                    signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
                )

            election1Phaser.arriveAndAwaitAdvanceWithTimeout()

            val firstLeaderAddress =
                apps.getRunningApps().firstNotNullOf { getLeaderAddress(it) }

            changePeers = {
                val peers =
                    apps.getRunningPeers(peerset().peersetId).mapValues { entry ->
                        val peer = entry.value
                        peer.copy(address = peer.address.replace(knownPeerIp, unknownPeerIp))
                    }
                apps.getApp(firstLeaderAddress.peerId).setPeerAddresses(peers)
            }

//      Start processing
            expectCatching {
                executeChange("${firstLeaderAddress.address}/v2/change/sync?peerset=peerset0&timeout=PT0.5S", change)
            }.isFailure()

            failurePhaser.arriveAndAwaitAdvanceWithTimeout()

            apps.getApp(firstLeaderAddress.peerId).stop(0, 0)

            election2Phaser.arriveAndAwaitAdvanceWithTimeout()
            changePhaser.arriveAndAwaitAdvanceWithTimeout()

            apps.getRunningPeers(peerset().peersetId)
                .values
                .filter { it != firstLeaderAddress }
                .forEach {
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
    fun `less than half of peers fails after electing leader`(): Unit =
        runBlocking {
            val allPeers = 5

            val electionPhaser = Phaser(1)
            val changePhaser = Phaser(allPeers - 2)
            listOf(electionPhaser, changePhaser).forEach { it.register() }

            val signalListener =
                mapOf(
                    Signal.PigPaxosLeaderElected to
                        SignalListener {
                            logger.info("Arrived at election ${it.subject.getPeerName()}")
                            electionPhaser.arrive()
                        },
                    Signal.PigPaxosChangeCommitted to
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

            val peerAddresses = apps.getRunningPeers(peerset().peersetId).values

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            val firstLeaderAddress = getLeaderAddress(peers[0])

            val peersToStop = peerAddresses.filter { it != firstLeaderAddress }.take(2)
            peersToStop.forEach { apps.getApp(it.peerId).stop(0, 0) }
            val runningPeers = peerAddresses.filter { address -> address !in peersToStop }
            val change = createChange()

//      Start processing
            executeChange("${runningPeers.first().address}/v2/change/sync?peerset=peerset0", change)

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
            val allPeers = 5

            val electionPhaser = Phaser(1)
            val changePhaser = Phaser(allPeers - 4)
            listOf(electionPhaser, changePhaser).forEach { it.register() }

            val peerLeaderElected = SignalListener { electionPhaser.arrive() }
            val peerApplyChange =
                SignalListener {
                    logger.info("Arrived ${it.subject.getPeerName()}")
                    changePhaser.arrive()
                }

            val signalListener =
                mapOf(
                    Signal.PigPaxosLeaderElected to peerLeaderElected,
                    Signal.PigPaxosReceivedAccept to peerApplyChange,
                )

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners = (0..4).map { "peer$it" }.associateWith { signalListener },
                )
            val peers = apps.getRunningApps()

            val peerAddresses = apps.getRunningPeers(peerset().peersetId).values

            electionPhaser.arriveAndAwaitAdvanceWithTimeout()

            val leaderAddressMap = peers.map { getLeaderAddress(it) }.groupingBy { it }.eachCount()

            var firstLeaderAddress = leaderAddressMap.toList().first().first

            leaderAddressMap.forEach {
                if (it.value > leaderAddressMap[firstLeaderAddress]!!) firstLeaderAddress = it.key
            }

            val peersToStop = peerAddresses.filter { it != firstLeaderAddress }.take(3)
            peersToStop.forEach { apps.getApp(it.peerId).stop(0, 0) }
            val runningPeers = peerAddresses.filter { address -> address !in peersToStop }
            val change = createChange()

//      Start processing
            executeChange("${firstLeaderAddress!!.address}/v2/change/async?peerset=peerset0", change)

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
            var allPeers = 5

            var isNetworkDivided = false

            val election1Phaser = Phaser(1)
            allPeers -= 2
            val election2Phaser = Phaser(1)
            val change1Phaser = Phaser(3)
            val change2Phaser = Phaser(2)
            val abortChangePhaser = Phaser(5)
            listOf(
                election1Phaser,
                election2Phaser,
                change1Phaser,
                change2Phaser,
                abortChangePhaser,
            ).forEach { it.register() }

            val signalListener =
                mapOf(
                    Signal.PigPaxosLeaderElected to
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
                    Signal.PigPaxosChangeCommitted to
                        SignalListener {
                            if (change1Phaser.phase == 0) {
                                logger.info("Arrived at change 1 ${it.subject.getPeerName()}")
                                change1Phaser.arrive()
                            } else {
                                logger.info("Arrived at change 2 ${it.subject.getPeerName()}")
                                change2Phaser.arrive()
                            }
                        },
                    Signal.PigPaxosChangeAborted to
                        SignalListener {
                            logger.info("Arrived at abortChange ${it.subject.getPeerName()}")
                            abortChangePhaser.arrive()
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

            val peerAddresses = apps.getRunningPeers(peerset().peersetId).values
            val peerAddresses2 = apps.getRunningPeers(peerset().peersetId)

            election1Phaser.arriveAndAwaitAdvanceWithTimeout()

            logger.info("First election finished")

            val firstLeaderAddress = peers.firstNotNullOf { getLeaderAddress(it) }

            logger.info("First leader: $firstLeaderAddress")

            val notLeaderPeers = peerAddresses.filter { it != firstLeaderAddress }

            val firstHalf: List<PeerAddress> = listOf(firstLeaderAddress, notLeaderPeers.first())
            val secondHalf: List<PeerAddress> = notLeaderPeers.drop(1)

//      Divide network
            isNetworkDivided = true

            firstHalf.forEach { address ->
                val peers =
                    apps.getRunningPeers(peerset().peersetId).mapValues { entry ->
                        val peer = entry.value
                        if (secondHalf.contains(peer)) {
                            peer.copy(address = peer.address.replace(knownPeerIp, unknownPeerIp))
                        } else {
                            peer
                        }
                    }
                apps.getApp(address.peerId).setPeerAddresses(peers)
            }

            secondHalf.forEach { address ->
                val peers =
                    apps.getRunningPeers(peerset().peersetId).mapValues { entry ->
                        val peer = entry.value
                        if (firstHalf.contains(peer)) {
                            peer.copy(address = peer.address.replace(knownPeerIp, unknownPeerIp))
                        } else {
                            peer
                        }
                    }
                apps.getApp(address.peerId).setPeerAddresses(peers)
            }

            logger.info("Network divided")

            logger.info("Second election finished")

            val change1 = createChange(content = "change1")
            val change2 = createChange(content = "change2")

//      Run change in both halfs
            executeChange("${firstHalf.first().address}/v2/change/async?peerset=peerset0", change1)

            executeChange("${secondHalf.first().address}/v2/change/async?peerset=peerset0", change2)

            change1Phaser.arriveAndAwaitAdvanceWithTimeout()

            logger.info("After change 1")

            firstHalf.forEach {
                val proposedChanges = askForProposedChanges(it)
                val acceptedChanges = askForAcceptedChanges(it)
                logger.debug("Checking changes $it proposed: $proposedChanges accepted: $acceptedChanges")
                expect {
                    that(proposedChanges.size).isLessThanOrEqualTo(1)
                    that(acceptedChanges.size).isEqualTo(0)
                }
                if (proposedChanges.size == 1) {
                    expect {
                        that(proposedChanges.first()).isEqualTo(change1)
                        that((proposedChanges.first() as StandardChange).content).isEqualTo("change1")
                    }
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
                    that((acceptedChanges.first() as StandardChange).content).isEqualTo("change2")
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
                logger.info("Checking ${it.peerId} proposed: $proposedChanges accepted: $acceptedChanges")
                expect {
                    that(proposedChanges.size).isEqualTo(0)
                    that(acceptedChanges.size).isEqualTo(1)
                }
                expect {
                    that(acceptedChanges.first()).isEqualTo(change2)
                    that((acceptedChanges.first() as StandardChange).content).isEqualTo("change2")
                }
            }
        }

    @RetryingTest(3)
    fun `should synchronize on history if it was added outside of paxos`(): Unit =
        runBlocking {
            val phaserGPACPeer = Phaser(1)
            val phaserPigPaxosPeers = Phaser(5)
            val leaderElectedPhaser = Phaser(1)

            val isSecondGPAC = AtomicBoolean(false)

            listOf(phaserGPACPeer, phaserPigPaxosPeers, leaderElectedPhaser).forEach { it.register() }

            val change1 =
                StandardChange(
                    "change",
                    peersets = listOf(ChangePeersetInfo(peerset(), InitialHistoryEntry.getId())),
                )

            val change2 =
                StandardChange(
                    "change",
                    peersets =
                        listOf(
                            ChangePeersetInfo(
                                peerset(),
                                change1.toHistoryEntry(peerset()).getId(),
                            ),
                        ),
                )

            val peerGPACAction =
                SignalListener {
                    phaserGPACPeer.arrive()
                }

            val leaderElectedAction = SignalListener { leaderElectedPhaser.arrive() }

            val firstLeaderAction =
                SignalListener { signalData ->
                    val url = "http://${signalData.peerResolver.resolve(peerId(1)).address}/protocols/gpac/apply?peerset=peerset0"
                    runBlocking {
                        testHttpClient.post(url) {
                            contentType(ContentType.Application.Json)
                            accept(ContentType.Application.Json)
                            setBody(
                                Apply(
                                    signalData.transaction!!.ballotNumber,
                                    true,
                                    Accept.COMMIT,
                                    signalData.change!!,
                                ),
                            )
                        }.body<HttpResponse>().also {
                            logger.info("Got response ${it.status.value}")
                        }
                    }
                    throw RuntimeException("Stop leader after apply")
                }

            val consensusPeersAction =
                SignalListener {
                    logger.info("Arrived: ${it.change}")
                    if (it.change == change2) phaserPigPaxosPeers.arrive()
                }

            val firstPeerSignals =
                mapOf(
                    Signal.BeforeSendingApply to firstLeaderAction,
                    Signal.PigPaxosLeaderElected to leaderElectedAction,
                    Signal.PigPaxosChangeCommitted to consensusPeersAction,
                    Signal.OnHandlingElectBegin to
                        SignalListener {
                            if (isSecondGPAC.get()) {
                                throw Exception("Ignore restarting GPAC")
                            }
                        },
                )

            val peerSignals =
                mapOf(
                    Signal.PigPaxosLeaderElected to leaderElectedAction,
                    Signal.PigPaxosChangeCommitted to consensusPeersAction,
                    Signal.OnHandlingElectBegin to SignalListener { if (isSecondGPAC.get()) throw Exception("Ignore restarting GPAC") },
                )

            val peerPaxosSignals =
                mapOf(
                    Signal.PigPaxosLeaderElected to leaderElectedAction,
                    Signal.PigPaxosChangeCommitted to consensusPeersAction,
                    Signal.OnHandlingElectBegin to SignalListener { if (isSecondGPAC.get()) throw Exception("Ignore restarting GPAC") },
                    Signal.OnHandlingAgreeBegin to SignalListener { throw Exception("Ignore GPAC") },
                )

            val peer1Signals =
                mapOf(
                    Signal.PigPaxosLeaderElected to leaderElectedAction,
                    Signal.PigPaxosChangeCommitted to consensusPeersAction,
                    Signal.OnHandlingApplyCommitted to peerGPACAction,
                )

            apps =
                TestApplicationSet(
                    mapOf(
                        "peerset0" to listOf("peer0", "peer1", "peer2", "peer3", "peer4"),
                    ),
                    signalListeners =
                        mapOf(
                            peer(0) to firstPeerSignals,
                            peer(1) to peer1Signals,
                            peer(2) to peerSignals,
                            peer(3) to peerPaxosSignals,
                            peer(4) to peerPaxosSignals,
                        ),
                    configOverrides =
                        mapOf(
                            peer(0) to mapOf("gpac.maxLeaderElectionTries" to 2),
                            peer(1) to mapOf("gpac.maxLeaderElectionTries" to 2),
                            peer(2) to mapOf("gpac.maxLeaderElectionTries" to 2),
                            peer(3) to mapOf("gpac.maxLeaderElectionTries" to 2),
                            peer(4) to mapOf("gpac.maxLeaderElectionTries" to 2),
                        ),
                )

            leaderElectedPhaser.arriveAndAwaitAdvanceWithTimeout()

            // change committed on 3/5 peers
            try {
                executeChange(
                    "${apps.getPeer(peer(0)).address}/v2/change/sync?peerset=peerset0&enforce_gpac=true",
                    change1,
                )
                fail("Change passed")
            } catch (e: Exception) {
                logger.info("Leader 1 fails", e)
            }

            // leader timeout is 5 seconds for integration tests - in the meantime other peer should wake up and execute transaction
            phaserGPACPeer.arriveAndAwaitAdvanceWithTimeout()
            isSecondGPAC.set(true)

            val change =
                testHttpClient.get("http://${apps.getPeer(peer(1)).address}/change?peerset=peerset0") {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                }.body<Change>()

            expect {
                that(change).isA<StandardChange>()
                that((change as StandardChange).content).isEqualTo(change1.content)
            }

            executeChangeSync(peer(1), "peerset0", change2)

            phaserPigPaxosPeers.arriveAndAwaitAdvanceWithTimeout()

            apps.getRunningPeers(peerset().peersetId).forEach { (_, peerAddress) ->
                // and should not execute this change couple of times
                val changes =
                    testHttpClient.get("http://${peerAddress.address}/changes?peerset=peerset0") {
                        contentType(ContentType.Application.Json)
                        accept(ContentType.Application.Json)
                    }.body<Changes>()

                // only one change and this change shouldn't be applied two times
                expectThat(changes.size).isGreaterThanOrEqualTo(2)
                expect {
                    that(changes[0]).isA<StandardChange>()
                    that((changes[0] as StandardChange).content).isEqualTo(change1.content)
                }
                expect {
                    that(changes[1]).isA<StandardChange>()
                    that((changes[1] as StandardChange).content).isEqualTo(change2.content)
                }
            }
        }

    @RetryingTest(3)
    fun `process 50 changes, then one peer doesn't respond on 250 changes and finally synchronize on all`(): Unit =
        runBlocking {
            val peersWithoutLeader = 5
            var iter = 0
            val isFirstPartCommitted = AtomicBoolean(false)
            val isAllChangeCommitted = AtomicBoolean(false)
            var change = createChange()
            val firstPart = 100
            val secondPart = 400

            val leaderElectedPhaser = Phaser(1)
            val allPeerChangePhaser = Phaser(peersWithoutLeader)
            val changePhaser = Phaser(peersWithoutLeader - 1)
            val endingPhaser = Phaser(1)
            listOf(leaderElectedPhaser, allPeerChangePhaser, changePhaser, endingPhaser).forEach { it.register() }

            val peerLeaderElected =
                SignalListener {
                    if (leaderElectedPhaser.phase == 0) {
                        logger.info("Arrived leader elected ${it.subject.getPeerName()}")
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
                                Signal.PigPaxosLeaderElected to peerLeaderElected,
                                Signal.PigPaxosChangeCommitted to peerChangeAccepted,
                            )
                        } +
                            mapOf(
                                "peer4" to
                                    mapOf(
                                        Signal.PigPaxosLeaderElected to peerLeaderElected,
                                        Signal.PigPaxosChangeCommitted to ignoringPeerChangeAccepted,
                                        Signal.PigPaxosBeginHandleMessages to ignoreHeartbeat,
                                        Signal.PigPaxosTryToBecomeLeader to
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
                executeChangeSync("peer0", "peerset0", change)
                allPeerChangePhaser.arriveAndAwaitAdvanceWithTimeout(Duration.ofSeconds(30))
                iter += 1
                change = createChange(content = "change$it", parentId = change.toHistoryEntry(PeersetId("peerset0")).getId())
            }
            // when: peer1 executed change

            isFirstPartCommitted.set(true)

            repeat(secondPart) {
                executeChangeSync("peer0", "peerset0", change)
                changePhaser.arriveAndAwaitAdvanceWithTimeout(Duration.ofSeconds(30))
                iter += 1
                logger.info("Change second part moved $it")
                change =
                    createChange(content = "change${it + 1 + firstPart}", parentId = change.toHistoryEntry(PeersetId("peerset0")).getId())
            }

            isAllChangeCommitted.set(true)

            endingPhaser.arriveAndAwaitAdvanceWithTimeout(Duration.ofSeconds(30))

            askAllForChanges(peerAddresses.values).forEach { changes ->
                // then: there are two changes
                expectThat(changes.size).isEqualTo(firstPart + secondPart)
            }
        }

    @Disabled("Not supported for now")
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

                executeChange("$peerAddress/v2/change/sync?peerset=peerset$i", change1)
                executeChange("$peerAddress/v2/change/sync?peerset=peerset$i", change2)
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
    ): String =
        testHttpClient.post("http://$uri") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            setBody(change)
        }.body()

    private suspend fun genericAskForChange(
        suffix: String,
        peerAddress: PeerAddress,
    ): Changes =
        testHttpClient.get("http://${peerAddress.address}/protocols/paxos/$suffix?peerset=peerset0") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.body()

    private suspend fun askForChanges(peerAddress: PeerAddress): Changes =
        testHttpClient.get("http://${peerAddress.address}/v2/change?peerset=peerset0") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }.body()

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
        val leaderId = (app.getPeersetProtocols(PeersetId("peerset0")).consensusProtocol as PaxosProtocol).getLeaderId()
        return leaderId?.let { apps.getPeer(it).address }
    }

    private fun getLeaderAddress(app: ApplicationUcac): PeerAddress? {
        val leaderId = (app.getPeersetProtocols(PeersetId("peerset0")).consensusProtocol as PaxosProtocol).getLeaderId()
        return leaderId?.let { apps.getPeer(it) }
    }

    private fun peer(peerId: Int): String = "peer$peerId"

    private fun peerId(peerId: Int): PeerId = PeerId(peer(peerId))

    private fun peerset(): PeersetId = PeersetId("peerset0")

    companion object {
        private val logger = LoggerFactory.getLogger(PaxosSpec::class.java)
    }
}
