package com.github.davenury.ucac.consensus.raft

import com.github.davenury.common.AlreadyLockedException
import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.Metrics
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.common.ProtocolName
import com.github.davenury.common.TwoPCChange
import com.github.davenury.common.TwoPCStatus
import com.github.davenury.common.history.History
import com.github.davenury.common.history.HistoryEntry
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.common.txblocker.TransactionAcquisition
import com.github.davenury.common.txblocker.TransactionBlocker
import com.github.davenury.ucac.Config
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.SignalSubject
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.ProtocolTimerImpl
import com.github.davenury.ucac.common.structure.Subscribers
import com.github.davenury.ucac.consensus.ConsensusResponse
import com.github.davenury.ucac.consensus.VotedFor
import com.github.davenury.ucac.raftHttpClients
import com.github.davenury.ucac.utils.MdcProvider
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.plus
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.slf4j.MDCContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.Executors
import kotlin.collections.set
import kotlin.system.measureTimeMillis

/** @author Kamil Jarosz */
class RaftConsensusProtocolImpl(
    private val peersetId: PeersetId,
    private val history: History,
    private val ctx: ExecutorCoroutineDispatcher,
    private var peerResolver: PeerResolver,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap(), peerResolver),
    private val protocolClient: RaftProtocolClient,
    private val heartbeatTimeout: Duration = Duration.ofSeconds(4),
    private val heartbeatDelay: Duration = Duration.ofMillis(500),
    private val transactionBlocker: TransactionBlocker,
    private val isMetricTest: Boolean,
    private val maxChangesPerMessage: Int,
    private val subscribers: Subscribers?,
) : RaftConsensusProtocol, SignalSubject {
    constructor(
        peersetId: PeersetId,
        history: History,
        config: Config,
        ctx: ExecutorCoroutineDispatcher,
        peerResolver: PeerResolver,
        signalPublisher: SignalPublisher = SignalPublisher(emptyMap(), peerResolver),
        protocolClient: RaftProtocolClient,
        transactionBlocker: TransactionBlocker,
        subscribers: Subscribers?,
    ) : this(
        peersetId,
        history,
        ctx,
        peerResolver,
        signalPublisher,
        protocolClient,
        heartbeatTimeout = config.consensus.heartbeatTimeout,
        heartbeatDelay = config.consensus.leaderTimeout,
        transactionBlocker = transactionBlocker,
        config.metricTest,
        config.consensus.maxChangesPerMessage,
        subscribers,
    )

    private val mdcProvider = MdcProvider(mapOf("peerset" to peersetId.toString()))
    private val peerId = peerResolver.currentPeer()

    private var currentTerm: Int = 0
    private val peerToNextIndex: MutableMap<PeerId, PeerIndices> = mutableMapOf()
    private val peerToLastHeartbeat: MutableMap<PeerId, ConsensusHeartbeat> = mutableMapOf()

    // private val peerToDeferred: MutableMap<PeerId, Deferred<ConsensusResponse<ConsensusHeartbeatResponse?>>> =
    //     mutableMapOf()
    private val voteContainer: VoteContainer = VoteContainer()
    private var votedFor: VotedFor? = null
    private val changesToBePropagatedToLeader: ConcurrentLinkedDeque<ChangeToBePropagatedToLeader> =
        ConcurrentLinkedDeque()
    private var state: Ledger = Ledger(history)

    @Volatile
    private var role: RaftRole = RaftRole.Candidate
    private var timer = ProtocolTimerImpl(heartbeatTimeout, heartbeatTimeout, ctx)
    private var lastHeartbeatTime = Instant.now()

    private val mutex = Mutex()
    private val mutexChangeToBePropagatedToLeader = Mutex()
    private var executorService: ExecutorCoroutineDispatcher? = null
    private val leaderRequestExecutorService = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

    private val changeIdToCompletableFuture: MutableMap<String, CompletableFuture<ChangeResult>> = mutableMapOf()

    override fun otherConsensusPeers(): List<PeerAddress> {
        return peerResolver.getPeersFromPeerset(peersetId).filter { it.peerId != peerId }
    }

    override suspend fun begin(): Unit =
        mdcProvider.withMdc {
            logger.info("Starting raft, other peers: ${otherConsensusPeers().map { it.peerId }}")
            with(CoroutineScope(ctx)) {
                launch(MDCContext()) {
                    sendLeaderRequest()
                }
            }
        }

    override fun getPeerName() = peerId.toString()

    private suspend fun sendLeaderRequest() {
        if (executorService != null) {
            mutex.withLock {
                restartTimer(RaftRole.Candidate)
            }
            throw Exception("$peerId Try become leader before cleaning")
        }
        signalPublisher.signal(
            Signal.ConsensusTryToBecomeLeader,
            this@RaftConsensusProtocolImpl,
            mapOf(peersetId to otherConsensusPeers()),
            null,
        )
        mutex.withLock {
            currentTerm += 1
            role = RaftRole.Candidate
            votedFor = VotedFor(peerId)
        }

        logger.debug("Trying to become a leader in term $currentTerm")

        val lastIndex =
            state.proposedEntries.find { it.changeId == transactionBlocker.getChangeId() }?.entry?.getId()
                ?: state.commitIndex

        val responses =
            protocolClient.sendConsensusElectMe(
                otherConsensusPeers(),
                ConsensusElectMe(peerId, currentTerm, lastIndex),
            ).map { it.message }

        logger.debug("Responses from leader request: $responses in iteration $currentTerm")

        val maxTerm = responses.filterNotNull().maxOfOrNull { it.myTerm } ?: currentTerm
        assert(maxTerm >= currentTerm)
        currentTerm = maxTerm

        val positiveResponses = responses.filterNotNull().count { it.voteGranted }

        if (!isMoreThanHalf(positiveResponses) || responses.filterNotNull().any { !it.voteGranted }) {
            mutex.withLock {
                restartTimer(RaftRole.Candidate)
            }
            return
        }

        mutex.withLock {
            role = RaftRole.Leader
            votedFor = votedFor!!.copy(elected = true)
            assert(executorService == null)
            executorService = Executors.newCachedThreadPool().asCoroutineDispatcher()
        }

        logger.info("Leader elected: me (in term $currentTerm)")
        subscribers?.notifyAboutConsensusLeaderChange(peerId, peersetId)
        signalPublisher.signal(
            Signal.ConsensusLeaderIHaveBeenElected,
            this@RaftConsensusProtocolImpl,
            mapOf(peersetId to otherConsensusPeers()),
            null,
        )

        Metrics.bumpLeaderElection(peerId, peersetId)

        peerToNextIndex.keys.forEach {
            peerToNextIndex.putIfAbsent(it, PeerIndices(state.lastApplied, state.lastApplied))
            peerToNextIndex.replace(it, PeerIndices(state.lastApplied, state.lastApplied))
        }

        scheduleHeartbeatToPeers(true)
        propagateChangesToLeaderInCourtine(executorService!!)
    }

    override suspend fun handleRequestVote(
        peerId: PeerId,
        iteration: Int,
        lastLogId: String,
    ): ConsensusElectedYou {
        logger.debug(
            "Handling vote request: peerId=$peerId,iteration=$iteration,lastLogIndex=$lastLogId, " +
                "currentTerm=$currentTerm,lastApplied=${state.lastApplied}",
        )

        mutex.withLock {
            if (iteration < currentTerm || (iteration == currentTerm && votedFor != null)) {
                logger.debug(
                    "Denying vote for $peerId due to an old term ($iteration vs $currentTerm), " +
                        "I voted for ${votedFor?.id}",
                )
                return ConsensusElectedYou(this@RaftConsensusProtocolImpl.peerId, currentTerm, false)
            }
            currentTerm = iteration
            if (amILeader()) stopBeingLeader(iteration)

            val lastEntryId =
                state.proposedEntries.lastOrNull()?.entry?.getId()
                    ?: state.lastApplied

            val candidateIsOutdated: Boolean = state.isOlderEntryThanLastEntry(lastLogId)

            if (candidateIsOutdated) {
                logger.debug("Denying vote for $peerId due to an old index ($lastLogId vs $lastEntryId)")
                return ConsensusElectedYou(this@RaftConsensusProtocolImpl.peerId, currentTerm, false)
            }
            votedFor = VotedFor(peerId)
        }
        restartTimer()
        logger.debug("Voted for $peerId in term $iteration")
        return ConsensusElectedYou(this@RaftConsensusProtocolImpl.peerId, currentTerm, true)
    }

    private suspend fun newLeaderElected(
        leaderId: PeerId,
        term: Int,
    ) {
        logger.info("Leader elected: $leaderId (in term $term)")
        votedFor = VotedFor(leaderId, true)
//          DONE: Check if stop() function make sure if you need to wait to all job finish ->
//          fixed by setting executor to null and adding null condition in if
        if (role == RaftRole.Leader) stopBeingLeader(term)
        role = RaftRole.Follower
        currentTerm = term
        signalPublisher.signal(
            Signal.ConsensusLeaderElected,
            this@RaftConsensusProtocolImpl,
            mapOf(peersetId to otherConsensusPeers()),
            null,
        )

        Metrics.refreshLastHeartbeat()

        releaseBlockerFromPreviousTermChanges()

        state.removeNotAcceptedItems()
        propagateChangesToLeaderInCourtine()
    }

    private suspend fun propagateChangesToLeaderInCourtine(ctx: ExecutorCoroutineDispatcher = leaderRequestExecutorService) {
        withContext(ctx) {
            async {
                tryPropagatingChangesToLeader()
            }
        }
    }

    override suspend fun handleHeartbeat(heartbeat: ConsensusHeartbeat): ConsensusHeartbeatResponse =
        mutex.withLock {
            val size = heartbeat.logEntries.size
            if (size != 0) {
                logger.debug("Handling heartbeat from ${heartbeat.leaderId}, entries overall: $size")
            } else {
                logger.debug("Handling empty heartbeat from ${heartbeat.leaderId}")
            }
            heartbeat.logEntries.forEach {
                val entry = HistoryEntry.deserialize(it.serializedEntry)
                signalPublisher.signal(
                    signal = Signal.ConsensusFollowerHeartbeatReceived,
                    subject = this@RaftConsensusProtocolImpl,
                    peers = mapOf(peersetId to otherConsensusPeers()),
                    change = Change.fromHistoryEntry(entry),
                    historyEntry = entry,
                )
            }

            Metrics.registerTimerHeartbeat()
            val term = heartbeat.term
            val leaderCommitId = heartbeat.leaderCommitId
            val isUpdatedCommitIndex = leaderCommitId != state.commitIndex
            val proposedChanges = heartbeat.logEntries.map { it.toLedgerItem() }
            val prevEntryId = heartbeat.prevEntryId

            if (term < currentTerm || (term == currentTerm && votedFor != null && heartbeat.leaderId != votedFor?.id)) {
                logger.info("The received heartbeat is not from current leader ($term vs $currentTerm)")
                return@withLock ConsensusHeartbeatResponse(false, currentTerm)
            }

            //      Restart timer because we received heartbeat from proper leader
            if (currentTerm < term || votedFor == null || votedFor?.elected == false) {
                newLeaderElected(heartbeat.leaderId, term)
            }

            val leaderCurrentEntryIsOutdated: Boolean =
                history.getCurrentEntryId() != heartbeat.currentHistoryEntryId && history.containsEntry(heartbeat.currentHistoryEntryId)

            if (leaderCurrentEntryIsOutdated) {
                logger.warn(
                    "Received heartbeat from leader that has outdated history my commit - ${state.commitIndex} leader commit - $leaderCommitId",
                )
                return@withLock ConsensusHeartbeatResponse(false, currentTerm, isLeaderCurrentEntryOutdated = true)
            }

            lastHeartbeatTime = Instant.now()

            restartTimer()

            val notAppliedProposedChanges: List<LedgerItem> =
                proposedChanges.filter { state.isNotAppliedNorProposed(it.entry.getId()) }

            val prevLogEntryExists: Boolean = prevEntryId == null || state.checkIfItemExist(prevEntryId)

            val commitIndexEntryExists: Boolean =
                notAppliedProposedChanges
                    .find { it.entry.getId() == leaderCommitId }
                    ?.let { true }
                    ?: state.checkIfItemExist(leaderCommitId)

            val areProposedChangesIncompatible: Boolean =
                notAppliedProposedChanges.isNotEmpty() &&
                    notAppliedProposedChanges.any {
                        !history.isEntryCompatible(it.entry)
                    }

            val acceptedChangesFromProposed: List<LedgerItem> =
                notAppliedProposedChanges
                    .indexOfFirst { it.entry.getId() == leaderCommitId }
                    .let { notAppliedProposedChanges.take(it + 1) }

            when {
                !commitIndexEntryExists -> {
                    logger.info(
                        "I miss some entries to commit (I am behind), lastCommitedEntryId: ${state.commitIndex}",
                    )
                    return@withLock ConsensusHeartbeatResponse(
                        false,
                        currentTerm,
                        missingValues = true,
                        lastCommittedEntryId = state.commitIndex,
                    )
                }

                !prevLogEntryExists -> {
                    logger.info("The received heartbeat is missing some changes (I am behind)")
                    state.removeNotAcceptedItems()
                    return@withLock ConsensusHeartbeatResponse(
                        false,
                        currentTerm,
                        missingValues = true,
                        lastCommittedEntryId = state.commitIndex,
                    )
                }

                isUpdatedCommitIndex && transactionBlocker.isAcquiredByProtocol(ProtocolName.CONSENSUS) -> {
                    logger.debug(
                        "Received heartbeat when is blocked so only accepted changes, blocked on ${transactionBlocker.getChangeId()}",
                    )
                    updateLedger(heartbeat, leaderCommitId, acceptedChangesFromProposed)
                    return@withLock ConsensusHeartbeatResponse(true, currentTerm, true)
                }

                notAppliedProposedChanges.isNotEmpty() && transactionBlocker.isAcquired() -> {
                    logger.debug("Received heartbeat but is blocked, so can't accept proposed changes")
                    return@withLock ConsensusHeartbeatResponse(false, currentTerm, true)
                }

                isUpdatedCommitIndex && areProposedChangesIncompatible -> {
                    logger.debug("Received heartbeat but changes are incompatible, updated accepted changes")
                    updateLedger(heartbeat, leaderCommitId, acceptedChangesFromProposed)
                    return@withLock ConsensusHeartbeatResponse(true, currentTerm, incompatibleWithHistory = true)
                }

                areProposedChangesIncompatible -> {
                    logger.debug("Received heartbeat but changes are incompatible")
                    return@withLock ConsensusHeartbeatResponse(false, currentTerm, incompatibleWithHistory = true)
                }

                notAppliedProposedChanges.isNotEmpty() -> {
                    logger.debug("Introduce new changes")
                    val changeId = notAppliedProposedChanges.first().changeId
                    transactionBlocker.acquireReentrant(TransactionAcquisition(ProtocolName.CONSENSUS, changeId))
                }
            }

            logger.debug("Updating ledger with {}", notAppliedProposedChanges)

            updateLedger(heartbeat, leaderCommitId, notAppliedProposedChanges)

            propagateChangesToLeaderInCourtine()

            return@withLock ConsensusHeartbeatResponse(true, currentTerm)
        }

    private suspend fun updateLedger(
        heartbeat: ConsensusHeartbeat,
        leaderCommitId: String,
        proposedChanges: List<LedgerItem> = listOf(),
    ) {
        val updateResult: LedgerUpdateResult = state.updateLedger(leaderCommitId, proposedChanges)

        updateResult.acceptedItems.forEach { acceptedItem ->
            signalPublisher.signal(
                signal = Signal.ConsensusFollowerChangeAccepted,
                subject = this@RaftConsensusProtocolImpl,
                peers = mapOf(peersetId to otherConsensusPeers()),
                change = Change.fromHistoryEntry(acceptedItem.entry),
                historyEntry = acceptedItem.entry,
            )
        }

        updateResult.proposedItems.forEach { proposedItem ->
            if (isMetricTest) {
                Metrics.bumpChangeMetric(
                    changeId = proposedItem.changeId,
                    peerId = peerId,
                    peersetId = peersetId,
                    protocolName = ProtocolName.CONSENSUS,
                    state = "proposed",
                )
            }
            signalPublisher.signal(
                signal = Signal.ConsensusFollowerChangeProposed,
                subject = this@RaftConsensusProtocolImpl,
                peers = mapOf(peersetId to otherConsensusPeers()),
                change = Change.fromHistoryEntry(proposedItem.entry),
                historyEntry = proposedItem.entry,
            )
        }

        updateResult.acceptedItems.forEach {
            if (isMetricTest) {
                Metrics.bumpChangeMetric(
                    changeId = it.changeId,
                    peerId = peerId,
                    peersetId = peersetId,
                    protocolName = ProtocolName.CONSENSUS,
                    state = "accepted",
                )
            }
            changeIdToCompletableFuture[it.changeId]?.complete(
                ChangeResult(
                    ChangeResult.Status.SUCCESS,
                    entryId = it.entry.getId(),
                ),
            )
        }

        updateResult.acceptedItems.find { it.changeId == transactionBlocker.getChangeId() }?.let {
            transactionBlocker.release(TransactionAcquisition(ProtocolName.CONSENSUS, it.changeId))
        }
    }

    override suspend fun handleProposeChange(change: Change): CompletableFuture<ChangeResult> =
        proposeChangeAsync(
            change,
        )

    override suspend fun getProposedChanges(): List<Change> =
        state.getLogEntries()
            .reversed()
            .takeWhile { it.entry.getId() != state.lastApplied }
            .mapNotNull { Change.fromHistoryEntry(it.entry) }

    override suspend fun getAcceptedChanges(): List<Change> =
        state.getLogEntries()
            .let { entries ->
                val index = entries.indexOfFirst { it.entry.getId() == state.lastApplied }
                history.toEntryList() + entries.take(index + 1).map { it.entry }
            }.mapNotNull { Change.fromHistoryEntry(it) }

    private suspend fun sendHeartbeatToPeer(
        peer: PeerId,
        isRegular: Boolean,
    ) {
        val peerAddress: PeerAddress
        peerAddress = peerResolver.resolve(peer)
        val peerMessage: ConsensusHeartbeat = getMessageForPeer(peerAddress)

        mutex.withLock {
            peerToLastHeartbeat[peer] = peerMessage
        }
        signalPublisher.signal(
            signal = Signal.ConsensusSendHeartbeat,
            subject = this@RaftConsensusProtocolImpl,
            peers = mapOf(peersetId to otherConsensusPeers()),
        )

        // We should schedule heartbeat even if something failed during handling response
        when {
            role != RaftRole.Leader ->
                logger.info("I am not longer leader so not schedule heartbeat again")

            !otherConsensusPeers().any { it.peerId == peer } ->
                logger.info("Peer $peer is not one of other consensus peer ${otherConsensusPeers()}")

            executorService == null ->
                logger.info("Executor service is null")

            !isRegular ->
                logger.debug("Heartbeat message is not regular")
        }

        if (isRegular && shouldISendHeartbeatToPeer(peer)) {
            launchHeartBeatToPeer(peer, true)
        }

        val response: ConsensusResponse<ConsensusHeartbeatResponse?>
        val deferred: Deferred<ConsensusResponse<ConsensusHeartbeatResponse?>>

        val idx = otherConsensusPeers().indexOf(peerAddress)

        val time =
            measureTimeMillis {
                deferred = protocolClient.sendConsensusHeartbeat(peerAddress, peerMessage, raftHttpClients[idx])

                // TODO since ktor 2.0.0 it doesn't work
                // mutex.withLock {
                //     peerToDeferred[peer]?.cancel()
                //     peerToDeferred[peer] = deferred
                // }

                response = deferred.await()
            }

        logger.debug("Peer $peer took $time ms to respond")

        when {
            response.message == null -> {
                logger.info("Peer $peer did not respond to heartbeat")
            }

            response.message.transactionBlocked && response.message.success -> {
                logger.info("Peer $peer has transaction blocker on but accepted changes was applied")
                handleSuccessHeartbeatResponseFromPeer(peerAddress, peerMessage.leaderCommitId)
            }

            response.message.success && response.message.incompatibleWithHistory -> {
                logger.info(
                    "Peer $peer's history is incompatible with proposed change but accepted some changes so retry proposing",
                )
                handleSuccessHeartbeatResponseFromPeer(peerAddress, peerMessage.leaderCommitId)
            }

            response.message.transactionBlocked -> {
                logger.info("Peer $peer has transaction blocker on")
            }

            response.message.incompatibleWithHistory -> {
                logger.info("Peer $peer's history is incompatible with proposed change")
//                TODO: fix this
                val removedEntries = state.checkIfProposedItemsAreStillValid()

                removedEntries.forEach {
                    val change = Change.fromHistoryEntry(it.entry)!!
                    changeIdToCompletableFuture[change.id]?.complete(
                        ChangeResult(
                            ChangeResult.Status.CONFLICT,
                            detailedMessage = "Change incompatible",
                            entryId = history.getCurrentEntryId(),
                        ),
                    )
                }

                logger.info("Entries removed: ${removedEntries.size}")
            }

            response.message.success -> {
                handleSuccessHeartbeatResponseFromPeer(
                    peerAddress,
                    peerMessage.leaderCommitId,
                    peerMessage.logEntries,
                )

                val isPeerMissingSomeEntries = peerToNextIndex[peer]?.acceptedEntryId != state.commitIndex

                if (isPeerMissingSomeEntries) {
                    launchHeartBeatToPeer(peer, delay = heartbeatDelay, sendInstantly = true)
                }
            }

            response.message.missingValues -> {
                logger.info(
                    "Peer ${peerAddress.peerId} is not up to date, decrementing index (their commit entry is ${response.message.lastCommittedEntryId})",
                )
                val oldValues = peerToNextIndex[peerAddress.peerId]
//                Done: Add decrement method for PeerIndices

                mutex.withLock {
                    peerToNextIndex[peerAddress.peerId] = oldValues
                        ?.let {
                            PeerIndices(
                                response.message.lastCommittedEntryId!!,
                                response.message.lastCommittedEntryId,
                            )
                        }
                        ?: PeerIndices()
                }
            }

            response.message.isLeaderCurrentEntryOutdated -> {
                logger.info("Peer ${peerAddress.peerId} did not accept a heartbeat, because I have outdated history")
            }

            response.message.term >= currentTerm && !response.message.success -> {
                logger.info("Based on info from $peer, someone else is currently a leader")
                mutex.withLock {
                    stopBeingLeader(response.message.term)
                    votedFor = null
                }
            }
        }

        checkIfQueuedChanges()
    }

    private fun shouldISendHeartbeatToPeer(peer: PeerId): Boolean = role == RaftRole.Leader && otherConsensusPeers().any { it.peerId == peer } && executorService != null

    private suspend fun applyAcceptedChanges(additionalAcceptedIds: List<String>) {
        val acceptedItems: List<LedgerItem>

        mutex.withLock {
            val acceptedIds: List<String> =
                voteContainer.getAcceptedChanges { isMoreThanHalf(it) } + additionalAcceptedIds
            acceptedItems = state.getLogEntries(acceptedIds)

            acceptedItems.forEach {
                signalPublisher.signal(
                    signal = Signal.ConsensusAfterProposingChange,
                    subject = this@RaftConsensusProtocolImpl,
                    peers = mapOf(peersetId to otherConsensusPeers()),
                    change = Change.fromHistoryEntry(it.entry),
                    historyEntry = it.entry,
                )
            }

            state.acceptItems(acceptedIds)

            acceptedItems.find { it.changeId == transactionBlocker.getChangeId() }?.let {
                transactionBlocker.tryRelease(TransactionAcquisition(ProtocolName.CONSENSUS, it.changeId))
            }

            voteContainer.removeChanges(acceptedIds)
        }

        acceptedItems.forEach {
            changeIdToCompletableFuture.putIfAbsent(it.changeId, CompletableFuture())
        }

        acceptedItems.forEach {
            changeIdToCompletableFuture[it.changeId]!!.complete(
                ChangeResult(
                    ChangeResult.Status.SUCCESS,
                    entryId = it.entry.getId(),
                ),
            )
        }

        if (acceptedItems.isNotEmpty()) scheduleHeartbeatToPeers(false)
    }

    private suspend fun stopBeingLeader(newTerm: Int) {
        logger.info("Some peer is a new leader in new term: $newTerm, currentTerm $currentTerm")
//       TODO: fix switch role and add test for it
        this.currentTerm = newTerm
        stopExecutorService()
        restartTimer()
    }

    //  TODO: Useless function
    private suspend fun checkIfQueuedChanges() {
        val change = changesToBePropagatedToLeader.poll() ?: return
        logger.info("Proposing a queued change (${changesToBePropagatedToLeader.size} left): $change")
        proposeChangeToLedger(change.cf, change.change)
    }

    private suspend fun handleSuccessHeartbeatResponseFromPeer(
        peerAddress: PeerAddress,
        leaderCommitId: String,
        newProposedChanges: List<LedgerItemDto> = listOf(),
    ) {
        val otherPeerId = peerAddress.peerId

        mutex.withLock {
            val peerIndices = peerToNextIndex.getOrDefault(otherPeerId, PeerIndices())
            if (newProposedChanges.isNotEmpty()) {
                newProposedChanges
                    .map { it.toLedgerItem() }
                    .filter { state.isNotApplied(it.entry.getId()) }
                    .forEach { voteContainer.voteForChange(it.entry.getId(), peerAddress) }

                newProposedChanges
                    .map { it.toLedgerItem() }
                    .filter { state.isNotApplied(it.entry.getId()) }
                    .forEach {
                        logger.debug(
                            "Votes after message to peer ${peerAddress.peerId}, ${
                                voteContainer.getVotes(
                                    it.entry.getId(),
                                )
                            } for entry: ${it.entry.getId()}",
                        )
                    }

                peerToNextIndex[otherPeerId] =
                    peerIndices.copy(
                        acknowledgedEntryId = newProposedChanges.last().toLedgerItem().entry.getId(),
                    )
            }

            if (peerIndices.acceptedEntryId != leaderCommitId) {
                val newPeerIndices = peerToNextIndex.getOrDefault(otherPeerId, PeerIndices())
                peerToNextIndex[otherPeerId] =
                    newPeerIndices.copy(acceptedEntryId = leaderCommitId)
            }
        }

        applyAcceptedChanges(listOf())
    }

    private suspend fun getMessageForPeer(peerAddress: PeerAddress): ConsensusHeartbeat {
        val peerIndices: PeerIndices
        val previousHeartbeat: ConsensusHeartbeat?

        val newCommittedChanges: List<LedgerItem>
        val newProposedChanges: List<LedgerItem>
        mutex.withLock {
            if (state.checkCommitIndex()) {
                peerToNextIndex.keys.forEach {
                    peerToNextIndex.replace(it, PeerIndices(state.lastApplied, state.lastApplied))
                }
            }

            peerIndices = peerToNextIndex.getOrDefault(peerAddress.peerId, PeerIndices())
            previousHeartbeat = peerToLastHeartbeat[peerAddress.peerId]
            if (previousHeartbeat != null && peerIndices.acknowledgedEntryId != previousHeartbeat.leaderCommitId && previousHeartbeat.logEntries.size == maxChangesPerMessage) {
                return previousHeartbeat.copy(term = currentTerm, currentHistoryEntryId = history.getCurrentEntryId())
            }
            newCommittedChanges = state.getCommittedItems(peerIndices.acknowledgedEntryId)
            newProposedChanges = state.getNewProposedItems(peerIndices.acknowledgedEntryId)
        }

        val allChanges = newCommittedChanges + newProposedChanges
        val lastAppliedChangeId = peerIndices.acceptedEntryId

        val limitedCommittedChanges = newCommittedChanges.take(maxChangesPerMessage)
        val lastLimitedCommittedChange = limitedCommittedChanges.lastOrNull()
        val lastId = lastLimitedCommittedChange?.entry?.getId()

        val (leaderCommitId, changesToSend) =
            if (limitedCommittedChanges.size < allChanges.size && lastId != null) {
                Pair(lastId, limitedCommittedChanges)
            } else {
                Pair(state.commitIndex, allChanges)
            }

        return ConsensusHeartbeat(
            peerId,
            currentTerm,
            changesToSend.map { it.toDto() },
            lastAppliedChangeId,
            history.getCurrentEntryId(),
            leaderCommitId,
        )
    }

    private suspend fun restartTimer(role: RaftRole = RaftRole.Follower) {
        timer.cancelCounting()
        val text =
            when (role) {
//            DONE: Think about this case again -> changed to candidate
                RaftRole.Candidate -> {
                    timer = getHeartbeatTimer()
                    "Leader not chosen make a timeout and try once again"
                }

                RaftRole.Follower -> {
                    timer = getHeartbeatTimer()
                    "leader ${votedFor?.id} doesn't send heartbeat, start try to become leader"
                }

                else -> {
                    restartTimer()
                    throw AssertionError("Leader restarted timer")
                }
            }

        timer.startCounting {
            val differenceFromLastHeartbeat: Duration
            mutex.withLock {
                differenceFromLastHeartbeat = Duration.between(lastHeartbeatTime, Instant.now())
            }
            if (differenceFromLastHeartbeat > heartbeatTimeout) {
                signalPublisher.signal(
                    Signal.ConsensusLeaderDoesNotSendHeartbeat,
                    this,
                    mapOf(peersetId to otherConsensusPeers()),
                    null,
                )
                logger.info(text)
                sendLeaderRequest()
            } else {
                restartTimer(this.role)
            }
        }
    }

    //  TODO: sync change will have to use Condition/wait/notifyAll
    override suspend fun proposeChangeToLedger(
        result: CompletableFuture<ChangeResult>,
        change: Change,
    ) {
        var entry: HistoryEntry

        mutex.withLock {
            logger.debug("ProposedEntries: ${state.proposedEntries.size}")
            logger.info("Proposing change: $change")
            if (state.entryAlreadyProposed(change.toHistoryEntry(peersetId, history.getCurrentEntryId()))) {
                logger.info("Already proposed that change: $change")
//                    scheduleHeartbeatToPeers(isRegular = false)
                return
            }

            val updatedChange: Change = TwoPC.updateParentIdFor2PCCompatibility(change, history, peersetId)

            val acquisition = TransactionAcquisition(ProtocolName.CONSENSUS, updatedChange.id)
            try {
                // TODO Why the hell are we blocking TX when proposing changes to the leader?
                transactionBlocker.acquireReentrant(acquisition)
            } catch (e: AlreadyLockedException) {
                logger.debug("Another TX in progress, queueing the change")
                changesToBePropagatedToLeader.add(ChangeToBePropagatedToLeader(change, result))
                return
            }

            entry = updatedChange.toHistoryEntry(peersetId, history.getCurrentEntryId())

            if (isDuring2PAndChangeDoesntFinishIt(change)) {
                logger.info("Queued change, because is during 2PC")
                changesToBePropagatedToLeader.add(ChangeToBePropagatedToLeader(change, result))
                transactionBlocker.release(acquisition)
                return
            }

            if (!history.isEntryCompatible(entry)) {
                logger.info(
                    "Proposed change is incompatible. \n CurrentChange: ${
                        history.getCurrentEntryId()
                    } \n Change.parentId: ${
                        entry.getParentId()
                    }",
                )
                result.complete(
                    ChangeResult(
                        ChangeResult.Status.CONFLICT,
                        detailedMessage = "Change incompatible",
                        entryId = history.getCurrentEntryId(),
                    ),
                )
                logger.error("Change conflicted")
                transactionBlocker.release(acquisition)
                return
            }

            logger.debug("Propose change to ledger: $updatedChange")
            state.proposeEntry(entry, updatedChange.id)
            voteContainer.initializeChange(entry.getId())

            scheduleHeartbeatToPeers(false)
        }

        if (otherConsensusPeers().isEmpty()) {
            logger.debug("No other consensus peers, applying change")
            applyAcceptedChanges(listOf(entry.getId()))
        }
    }

    private suspend fun recoveryEntry(changeId: String) {
        // TODO What the hell is going on in here?
        val blockedEntry = state.getLogEntries().firstOrNull { Change.fromHistoryEntry(it.entry)?.id == changeId }

        if (blockedEntry == null) {
            logger.info("Change $changeId was lost, release transactionBlocker")
            transactionBlocker.tryRelease(TransactionAcquisition(ProtocolName.CONSENSUS, changeId))
            return
        }

        if (history.containsEntry(blockedEntry.entry.getId()) || !history.isEntryCompatible(blockedEntry.entry)) {
            logger.info("Blocked entry is no longer valid")
            transactionBlocker.tryRelease(TransactionAcquisition(ProtocolName.CONSENSUS, changeId))
            return
        }

        logger.info("We must inform leader about blocked entry")

        changesToBePropagatedToLeader
            .addFirst(
                ChangeToBePropagatedToLeader(
                    Change.fromHistoryEntry(blockedEntry.entry)!!,
                    changeIdToCompletableFuture[changeId]!!,
                ),
            )
        propagateChangesToLeaderInCourtine()
    }

    private fun isDuring2PAndChangeDoesntFinishIt(change: Change): Boolean {
        val currentHistoryChange =
            history
                .getCurrentEntry()
                .let { Change.fromHistoryEntry(it) }

        val isDuring2PC: Boolean =
            currentHistoryChange
                ?.let { it is TwoPCChange && it.twoPCStatus == TwoPCStatus.ACCEPTED }
                ?: false

        val changeIsAbortingOf2PC: Boolean =
            change is TwoPCChange && change.twoPCStatus == TwoPCStatus.ABORTED

        val changeIsApplyingOf2PC: Boolean =
            currentHistoryChange is TwoPCChange && change.id ==
                currentHistoryChange.change.id

        return isDuring2PC && !(changeIsAbortingOf2PC || changeIsApplyingOf2PC)
    }

    private fun releaseBlockerFromPreviousTermChanges() {
        val blockedChange =
            state.proposedEntries.find {
                it.changeId == transactionBlocker.getChangeId()
            } ?: return

        if (transactionBlocker.tryRelease(TransactionAcquisition(ProtocolName.CONSENSUS, blockedChange.changeId))) {
            logger.info("Released blocker for changes from previous term")

            changeIdToCompletableFuture[state.proposedEntries.last().changeId]
                ?.complete(
                    ChangeResult(
                        ChangeResult.Status.TIMEOUT,
                        entryId = null,
                    ),
                )
        }
    }

    private suspend fun sendRequestToLeader(
        cf: CompletableFuture<ChangeResult>,
        change: Change,
    ) {
        with(CoroutineScope(Dispatchers.IO)) {
            async(MDCContext()) {
                var result: ChangeResult? = null
//              It won't be infinite loop because if leader exists we will finally send message to him and if not we will try to become one
                while (result == null) {
                    val address: String
                    if (votedFor == null || votedFor!!.id == peerId) {
                        logger.info("Queueing the change, there's no leader")
                        changesToBePropagatedToLeader.add(ChangeToBePropagatedToLeader(change, cf))
                        return@async
                    } else {
                        address = peerResolver.resolve(votedFor!!.id).address
                    }
                    logger.info("Propagating change ${change.id} to the leader ${votedFor?.id}")

                    result =
                        try {
                            val changeResult: ChangeResult
                            measureTimeMillis {
                                changeResult = protocolClient.sendRequestApplyChange(address, change)
                            }.also { logger.info("Sending message to leader took $it ms") }
                            changeResult
                        } catch (e: Exception) {
                            logger.error("Request to leader ($address, ${votedFor?.id}) failed", e)
                            null
                        }
                    if (result == null) delay(heartbeatDelay.toMillis())
                }
                if (result.status != ChangeResult.Status.SUCCESS) {
                    cf.complete(result)
                }
            }
        }
    }

    //   TODO: only one change can be proposed at the same time
    override suspend fun proposeChangeAsync(change: Change): CompletableFuture<ChangeResult> {
        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
        val result = changeIdToCompletableFuture[change.id]!!
        measureTimeMillis {
            when {
                amILeader() -> {
                    proposeChangeToLedger(result, change)
                }

                votedFor?.elected == true -> {
                    logger.debug("Forwarding change to the leader(${votedFor!!}): $change")
                    sendRequestToLeader(result, change)
                }
                else -> {
                    logger.debug("Queueing a change to be propagated when leader is elected")
                    changesToBePropagatedToLeader.add(ChangeToBePropagatedToLeader(change, result))
                }
            }
        }.also { logger.debug("ProposeChangeAsync took $it ms") }
        return result
    }

    private fun scheduleHeartbeatToPeers(isRegular: Boolean) {
        otherConsensusPeers().forEach {
            launchHeartBeatToPeer(it.peerId, isRegular, true)
        }
    }

    private fun launchHeartBeatToPeer(
        peer: PeerId,
        isRegular: Boolean = false,
        sendInstantly: Boolean = false,
        delay: Duration = heartbeatDelay,
    ) {
        if (shouldISendHeartbeatToPeer(peer)) {
            CoroutineScope(executorService!!).async(MDCContext()) {
                if (!sendInstantly) {
                    logger.debug("Wait with sending heartbeat to $peer for ${delay.toMillis()} ms")
                    delay(delay.toMillis())
                }
                sendHeartbeatToPeer(peer, isRegular)
            }
        }
    }

    override fun getState(): History {
        val history: History
        runBlocking {
            mutex.withLock {
                history = state.getHistory()
                logger.info("Request for state: $history")
            }
        }
        return history
    }

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? = changeIdToCompletableFuture[changeId]

    override fun stop() =
        mdcProvider.withMdc {
            logger.info("Stopping whole consensus")
            stopExecutorService()
            leaderRequestExecutorService.cancel()
            leaderRequestExecutorService.close()
        }

    private fun stopExecutorService() {
        executorService?.cancel()
        executorService?.close()
        executorService = null
    }

    override fun amILeader(): Boolean = role == RaftRole.Leader

    private fun getHeartbeatTimer() = ProtocolTimerImpl(heartbeatTimeout, heartbeatTimeout.dividedBy(2), ctx)

    private suspend fun tryPropagatingChangesToLeader() {
        // TODO mutex?
        val votedFor = this.votedFor
        if (votedFor == null || !votedFor.elected) return
        val changeCount = changesToBePropagatedToLeader.size
        if (changeCount > 0) logger.info("Propagating {} changes to the leader", changeCount)
        while (true) {
            val changeToBePropagated: ChangeToBePropagatedToLeader
            mutexChangeToBePropagatedToLeader.withLock {
                changeToBePropagated = changesToBePropagatedToLeader.poll() ?: return
            }
            if (votedFor.id == peerId) {
                logger.debug("Processing a queued change as a leader: ${changeToBePropagated.change}")
                proposeChangeToLedger(changeToBePropagated.cf, changeToBePropagated.change)
            } else {
                logger.debug("Propagating a change to the leader (${votedFor.id}): ${changeToBePropagated.change}")
                sendRequestToLeader(changeToBePropagated.cf, changeToBePropagated.change)
            }
        }
    }

    override fun getLeaderId(): PeerId? = votedFor?.let { if (it.elected) it.id else null }

    companion object {
        private val logger = LoggerFactory.getLogger("raft")
    }
}

data class PeerIndices(
    val acceptedEntryId: String = InitialHistoryEntry.getId(),
    val acknowledgedEntryId: String = InitialHistoryEntry.getId(),
) {
    suspend fun decrement(ledger: Ledger): PeerIndices =
        ledger
            .getPreviousEntryId(acceptedEntryId)
            .let { PeerIndices(it, it) }
}

data class ChangeToBePropagatedToLeader(
    val change: Change,
    val cf: CompletableFuture<ChangeResult>,
)
