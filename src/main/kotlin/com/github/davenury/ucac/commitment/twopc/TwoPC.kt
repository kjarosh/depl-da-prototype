package com.github.davenury.ucac.commitment.twopc

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeDoesntExist
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.HistoryCannotBeBuildException
import com.github.davenury.common.Metrics
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.common.ProtocolName
import com.github.davenury.common.TwoPCChange
import com.github.davenury.common.TwoPCConflictException
import com.github.davenury.common.TwoPCHandleException
import com.github.davenury.common.TwoPCStatus
import com.github.davenury.common.history.History
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.SignalSubject
import com.github.davenury.ucac.TwoPCConfig
import com.github.davenury.ucac.commitment.AbstractAtomicCommitmentProtocol
import com.github.davenury.ucac.common.ChangeNotifier
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.ProtocolTimer
import com.github.davenury.ucac.common.ProtocolTimerImpl
import com.github.davenury.ucac.consensus.ConsensusProtocol
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.future.await
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

class TwoPC(
    private val peersetId: PeersetId,
    private val history: History,
    twoPCConfig: TwoPCConfig,
    ctx: ExecutorCoroutineDispatcher,
    private val protocolClient: TwoPCProtocolClient,
    private val consensusProtocol: ConsensusProtocol,
    peerResolver: PeerResolver,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap(), peerResolver),
    private val isMetricTest: Boolean,
    private val changeNotifier: ChangeNotifier,
) : SignalSubject, AbstractAtomicCommitmentProtocol(logger, peerResolver) {
    private val peerId = peerResolver.currentPeer()

    private var changeTimer: ProtocolTimer = ProtocolTimerImpl(twoPCConfig.changeDelay, Duration.ZERO, ctx)
    val currentConsensusLeaders = ConcurrentHashMap<PeersetId, PeerAddress>()

    override suspend fun performProtocol(change: Change) {
        val updatedChange =
            updateParentIdFor2PCCompatibility(change, history, peersetId)

        logger.info("Performing a 2PC change as a leader: $change")

        val mainChangeId = change.id
        try {
            val acceptChange =
                TwoPCChange(
                    peersets = change.peersets,
                    twoPCStatus = TwoPCStatus.ACCEPTED,
                    change = change,
                    leaderPeerset = peersetId,
                )

            val otherPeersets =
                updatedChange.peersets
                    .map { it.peersetId }
                    .filter { it != peersetId }
            val otherPeers: Map<PeersetId, PeerAddress> =
                otherPeersets.associateWith { currentConsensusLeaders[it] ?: peerResolver.getPeersFromPeerset(it)[0] }

            signal(Signal.TwoPCBeforeProposePhase, change)
            val (decision, parentId) = proposePhase(acceptChange, mainChangeId, otherPeers)

            if (isMetricTest) {
                Metrics.bumpChangeMetric(
                    changeId = mainChangeId,
                    peerId = peerId,
                    peersetId = peersetId,
                    protocolName = ProtocolName.TWO_PC,
                    state = "proposed_decision_$decision",
                )
            }

            signal(Signal.TwoPCOnChangeAccepted, change)
            val decisionPhaseOtherPeers =
                otherPeersets.associateWith {
                    currentConsensusLeaders[it] ?: peerResolver.getPeersFromPeerset(it)[0]
                }
            val consensusResult = decisionPhase(acceptChange, decision, decisionPhaseOtherPeers, parentId)

            val result = if (decision) ChangeResult.Status.SUCCESS else ChangeResult.Status.ABORTED

            postDecisionOperations(mainChangeId, change, result, consensusResult)
        } catch (e: Exception) {
            changeIdToCompletableFuture[mainChangeId]!!.complete(ChangeResult(ChangeResult.Status.CONFLICT))
        }
    }

    private fun postDecisionOperations(
        mainChangeId: String,
        change: Change,
        result: ChangeResult.Status,
        consensusResult: ChangeResult,
    ) {
        if (isMetricTest) {
            Metrics.bumpChangeMetric(
                changeId = mainChangeId,
                peerId = peerId,
                peersetId = peersetId,
                protocolName = ProtocolName.TWO_PC,
                state = result.name.lowercase(),
            )
        }

        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
        changeIdToCompletableFuture[change.id]!!.complete(
            ChangeResult(
                result,
                entryId = consensusResult.entryId,
            ),
        )
        signal(Signal.TwoPCOnChangeApplied, change)
    }

    suspend fun handleAccept(change: Change) {
        if (change !is TwoPCChange) {
            logger.error("Received not a 2PC change $change")
            throw TwoPCHandleException("Received change of not TwoPCChange in handleAccept: $change")
        }

        logger.debug("Change id for change: $change, id: ${change.change.id}")
        changeIdToCompletableFuture.putIfAbsent(change.change.id, CompletableFuture<ChangeResult>())

        val changeWithProperParentId =
            change.copyWithNewParentId(
                peersetId,
                history.getCurrentEntryId(),
            )

        logger.info("Proposing locally (as a subordinate) ${change.id}")
        val result =
            consensusProtocol.proposeChangeAsync(changeWithProperParentId).await()

        if (result.status != ChangeResult.Status.SUCCESS) {
            throw TwoPCHandleException("TwoPCChange didn't apply change")
        }

        logger.info("Proposed locally (as a subordinate) ${change.id}")
        changeTimer.startCounting {
            askForDecisionChange(change)
        }
    }

    private suspend fun askForDecisionChange(change: Change) {
        val otherPeerset =
            change.peersets.map { it.peersetId }
                .first { it != peersetId }

        signal(Signal.TwoPCOnAskForDecision, change)
        val resultChange =
            protocolClient.askForChangeStatus(
                otherPeerset.let { peerResolver.getPeersFromPeerset(it)[0] },
                change,
                otherPeerset,
            )

        logger.debug("Asking about change: $change - result - $resultChange")
        if (resultChange != null) {
            handleDecision(resultChange)
        } else {
            changeTimer.startCounting {
                askForDecisionChange(change)
            }
        }
    }

    suspend fun handleDecision(change: Change) {
        logger.info("Handling decision: $change")

        signal(Signal.TwoPCOnHandleDecision, change)
        val mainChangeId =
            updateParentIdFor2PCCompatibility(change, history, peersetId).id
        logger.debug("Change id for change: $change, id: $mainChangeId")

        val currentProcessedChange = Change.fromHistoryEntry(history.getCurrentEntry())

        val consensusResult: ChangeResult
        try {
            val cf: CompletableFuture<ChangeResult> =
                when {
                    currentProcessedChange !is TwoPCChange || currentProcessedChange.twoPCStatus != TwoPCStatus.ACCEPTED -> {
                        throw TwoPCHandleException(
                            "Received change in handleDecision even though we didn't received 2PC-Accept earlier",
                        )
                    }

                    change is TwoPCChange && change.twoPCStatus == TwoPCStatus.ABORTED && change.change == currentProcessedChange.change -> {
                        changeTimer.cancelCounting()
                        val updatedChange =
                            change.copyWithNewParentId(
                                peersetId,
                                history.getCurrentEntryId(),
                            )
                        logger.info("Aborting locally (as a subordinate) ${change.id}")
                        checkChangeAndProposeToConsensus(updatedChange, currentProcessedChange.change.id)
                    }

                    change == currentProcessedChange.change -> {
                        changeTimer.cancelCounting()
                        val updatedChange =
                            change.copyWithNewParentId(
                                peersetId,
                                history.getCurrentEntryId(),
                            )
                        logger.info("Committing locally (as a subordinate) ${change.id}")
                        checkChangeAndProposeToConsensus(updatedChange, currentProcessedChange.change.id)
                    }

                    else -> throw TwoPCHandleException(
                        "In 2PC handleDecision received change in different type than TwoPCChange: $change \n" +
                            "currentProcessedChange: $currentProcessedChange",
                    )
                }
            consensusResult = cf.await()
            signal(Signal.TwoPCOnHandleDecisionEnd, change)
        } catch (e: Exception) {
            logger.error("Error committing change", e)
            changeConflict(mainChangeId, "Change conflicted in decision phase, ${e.message}")
            throw e
        }

        val result =
            ChangeResult(
                ChangeResult.Status.SUCCESS,
                entryId = consensusResult.entryId,
            )
        changeNotifier.notify(change, result)
        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
        changeIdToCompletableFuture[change.id]!!.complete(result)
    }

    fun getChange(changeId: String): Change {
        val entryList = consensusProtocol.getState().toEntryList()

        val parentEntry =
            entryList
                .find { Change.fromHistoryEntry(it)?.id == changeId }
                ?: throw ChangeDoesntExist(changeId)

        val change =
            entryList
                .find { it.getParentId() == parentEntry.getId() }
                ?.let { Change.fromHistoryEntry(it) }
                ?: throw ChangeDoesntExist(changeId)

        // TODO hax, remove it
        return if (change is TwoPCChange) {
            change
        } else {
            Change.fromHistoryEntry(parentEntry)
                .let {
                    if (it !is TwoPCChange) {
                        throw ChangeDoesntExist(changeId)
                    } else {
                        it
                    }
                }.change
        }
    }

    // I have been selected as a new leader, let me check if there are any 2PC changes
    fun newConsensusLeaderElected(
        peerId: PeerId,
        peersetId: PeersetId,
    ) {
        logger.info("I have been selected as a new consensus leader")
        val currentChange = Change.fromHistoryEntry(history.getCurrentEntry())
        logger.info("Current change in history is - $currentChange")
        if (currentChange !is TwoPCChange || currentChange.twoPCStatus == TwoPCStatus.ABORTED) {
            // everything good - current change is not 2PC change
            logger.info("There's no unfinished TwoPC changes, I can receive new changes")
            return
        }
        // try to find out what happened to the transaction
        if (currentChange.leaderPeerset == peersetId) {
            logger.info("Change $currentChange is my change, so I need to go with decision phase")
            // it was my change, so it's better to finish it
            runBlocking {
                // TODO - it does not have to be false - it was a fault so it's safer to abort the transaction
                val consensusResult =
                    decisionPhase(
                        currentChange,
                        false,
                        currentChange.peersets.filterNot { it.peersetId == peersetId }
                            .associate { it.peersetId to peerResolver.getPeersFromPeerset(it.peersetId)[0] },
                        // TODO We should get the parentId somehow here
                        null,
                    )

                postDecisionOperations(
                    updateParentIdFor2PCCompatibility(currentChange.change, history, peersetId).id,
                    currentChange.change,
                    ChangeResult.Status.ABORTED,
                    consensusResult,
                )
            }
        } else {
            logger.info("Change $currentChange is not my change, I'll ask about it")
            // it was not my change, so I should ask about it
            runBlocking {
                askForDecisionChange(currentChange)
            }
        }
    }

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? = changeIdToCompletableFuture[changeId]

    private suspend fun proposePhase(
        acceptChange: TwoPCChange,
        mainChangeId: String,
        otherPeers: Map<PeersetId, PeerAddress>,
    ): Pair<Boolean, String?> {
        logger.info("Proposing locally $mainChangeId")
        val acceptResult = checkChangeAndProposeToConsensus(acceptChange, mainChangeId).await()
        if (acceptResult.status != ChangeResult.Status.SUCCESS) {
            changeConflict(mainChangeId, "failed during processing acceptChange in 2PC")
        } else {
            logger.info("Change accepted locally ${acceptChange.change}")
        }

        logger.info("Proposing remotely $mainChangeId")
        val decision = getProposePhaseResponses(otherPeers, acceptChange, mapOf())

        logger.info("Decision $decision from other peerset for ${acceptChange.change}")

        return Pair(decision, acceptResult.entryId)
    }

    suspend fun getProposePhaseResponses(
        peers: Map<PeersetId, PeerAddress>,
        change: Change,
        recentResponses: Map<PeerAddress, TwoPCRequestResponse>,
    ): Boolean {
        val responses = protocolClient.sendAccept(peers, change)

        val addressesToAskAgain =
            responses.filter { (_, response) -> response.redirect }
                .map { (_, response) ->
                    val address = peerResolver.resolve(response.newConsensusLeaderId!!)
                    logger.debug(
                        "Updating {} peerset to new consensus leader: {}",
                        response.newConsensusLeaderPeersetId,
                        response.newConsensusLeaderId,
                    )
                    currentConsensusLeaders[response.newConsensusLeaderPeersetId!!] = address
                    response.peersetId to address
                }.toMap()

        if (addressesToAskAgain.isEmpty()) {
            return (recentResponses + responses).values.map { it.success }.all { it }
        }

        logger.error("Asking some peers again: $addressesToAskAgain")
        return getProposePhaseResponses(
            addressesToAskAgain,
            change,
            (recentResponses + responses.filterNot { it.value.redirect }),
        )
    }

    private suspend fun decisionPhase(
        acceptChange: TwoPCChange,
        decision: Boolean,
        otherPeers: Map<PeersetId, PeerAddress>,
        parentId: String?,
    ): ChangeResult {
        val change = acceptChange.change

        logger.info("Decision phase (decision=$decision): ${change.id}")

        val commitChange =
            if (decision) {
                change
            } else {
                TwoPCChange(
                    peersets = change.peersets,
                    twoPCStatus = TwoPCStatus.ABORTED,
                    change = change,
                    leaderPeerset = peersetId,
                )
            }
        logger.debug("Change to commit: $commitChange")

        logger.info("Committing remotely ${change.id}")
        protocolClient.sendDecision(otherPeers, commitChange)

        logger.info("Committing locally ${change.id}")
        val changeResult =
            checkChangeAndProposeToConsensus(
                commitChange.copyWithNewParentId(
                    peersetId,
                    parentId,
                ),
                acceptChange.change.id,
            ).await()

        if (changeResult.status != ChangeResult.Status.SUCCESS) {
            throw TwoPCConflictException("Change failed during committing locally")
        }

        logger.info("Decision $decision committed in all peersets $commitChange")

        return changeResult
    }

    private fun signal(
        signal: Signal,
        change: Change,
    ) {
        signalPublisher.signal(
            signal,
            this,
            getPeersFromChange(change),
            null,
            change,
        )
    }

    private fun checkChangeCompatibility(
        change: Change,
        originalChangeId: String,
    ) {
        if (!history.isEntryCompatible(change.toHistoryEntry(peersetId, history.getCurrentEntryId()))) {
            logger.info(
                "Change $originalChangeId is not compatible with history expected: ${
                    change.toHistoryEntry(peersetId).getParentId()
                } is ${history.getCurrentEntryId()}",
            )
            changeIdToCompletableFuture[originalChangeId]!!.complete(
                ChangeResult(ChangeResult.Status.REJECTED, entryId = history.getCurrentEntryId()),
            )
            throw HistoryCannotBeBuildException()
        }
    }

    private suspend fun checkChangeAndProposeToConsensus(
        change: Change,
        originalChangeId: String,
    ): CompletableFuture<ChangeResult> =
        change
            .also { checkChangeCompatibility(it, originalChangeId) }
            .let { consensusProtocol.proposeChangeAsync(change) }

    private fun changeConflict(
        changeId: String,
        exceptionText: String,
    ) = changeIdToCompletableFuture
        .also { it.putIfAbsent(changeId, CompletableFuture()) }
        .get(changeId)!!.complete(ChangeResult(ChangeResult.Status.CONFLICT))
        .also { throw TwoPCConflictException(exceptionText) }

    companion object {
        private val logger = LoggerFactory.getLogger("2pc")

        fun updateParentIdFor2PCCompatibility(
            change: Change,
            history: History,
            peersetId: PeersetId,
        ): Change = change
    }
}
