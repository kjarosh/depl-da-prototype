package com.github.davenury.ucac.commitment.gpac

import com.github.davenury.common.AlreadyLockedException
import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.Changes
import com.github.davenury.common.HistoryCannotBeBuildException
import com.github.davenury.common.Metrics
import com.github.davenury.common.NotElectingYou
import com.github.davenury.common.NotValidLeader
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.common.ProtocolName
import com.github.davenury.common.TransactionNotBlockedOnThisChange
import com.github.davenury.common.history.History
import com.github.davenury.common.txblocker.TransactionAcquisition
import com.github.davenury.common.txblocker.TransactionBlocker
import com.github.davenury.ucac.GpacConfig
import com.github.davenury.ucac.Signal
import com.github.davenury.ucac.SignalPublisher
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.common.ProtocolTimer
import com.github.davenury.ucac.common.ProtocolTimerImpl
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import kotlin.math.floor

class GPACProtocolImpl(
    private val peersetId: PeersetId,
    private val history: History,
    private val gpacConfig: GpacConfig,
    ctx: ExecutorCoroutineDispatcher,
    private val protocolClient: GPACProtocolClient,
    private val transactionBlocker: TransactionBlocker,
    peerResolver: PeerResolver,
    private val signalPublisher: SignalPublisher = SignalPublisher(emptyMap(), peerResolver),
    private val isMetricTest: Boolean = false,
) : GPACProtocolAbstract(peerResolver, logger) {
    private val peerId: PeerId = peerResolver.currentPeer()

    var leaderTimer: ProtocolTimer = ProtocolTimerImpl(gpacConfig.leaderFailDelay, gpacConfig.leaderFailBackoff, ctx)
    var retriesTimer: ProtocolTimer =
        ProtocolTimerImpl(gpacConfig.initialRetriesDelay, gpacConfig.retriesBackoffTimeout, ctx)
    private val maxLeaderElectionTries = gpacConfig.maxLeaderElectionTries

    private var myBallotNumber: Int = 0

    private var transaction: Transaction = Transaction(myBallotNumber, Accept.ABORT, change = null)
    private val phaseMutex = Mutex()

    private fun isValidBallotNumber(ballotNumber: Int): Boolean = ballotNumber > myBallotNumber

    override fun getTransaction(): Transaction = this.transaction

    override fun getBallotNumber(): Int = myBallotNumber

    override suspend fun handleElect(message: ElectMe): ElectedYou {
        return phaseMutex.withLock {
            logger.debug("Handling elect {}", message)
            signal(Signal.OnHandlingElectBegin, null, message.change)

            val acquisition = transactionBlocker.getAcquisition()
            if (acquisition != null && acquisition.changeId != message.change.id) {
                logger.error("Tried to respond to elect me when semaphore acquired!")
                throw AlreadyLockedException(acquisition)
            }

            if (!isValidBallotNumber(message.ballotNumber)) {
                logger.error(
                    "Ballot number is invalid - my ballot number: $myBallotNumber, received: ${message.ballotNumber}",
                )
                throw NotElectingYou(myBallotNumber, message.ballotNumber)
            }

            val entry = message.change.toHistoryEntry(peersetId)
            var initVal = if (history.isEntryCompatible(entry)) Accept.COMMIT else Accept.ABORT
            logger.debug("Elect init val: {}", initVal)
            if (gpacConfig.abortOnElectMe) {
                initVal = Accept.ABORT
            }

            myBallotNumber = message.ballotNumber

            signal(Signal.OnHandlingElectEnd, transaction, message.change)
            this@GPACProtocolImpl.transaction = transaction.copy(initVal = initVal)

            if (isMetricTest) {
                Metrics.bumpChangeMetric(
                    changeId = message.change.id,
                    peerId = peerId,
                    peersetId = peersetId,
                    protocolName = ProtocolName.GPAC,
                    state = "leader_elected",
                )
            }
            return@withLock ElectedYou(
                message.ballotNumber,
                initVal,
                transaction.acceptNum,
                transaction.acceptVal,
                transaction.decision,
            )
        }
    }

    override suspend fun handleAgree(message: Agree): Agreed {
        return phaseMutex.withLock {
            logger.info("Handling agree")
            if (this@GPACProtocolImpl.transaction.decision) {
                logger.info("There's a decision")
                return Agreed(message.ballotNumber, this@GPACProtocolImpl.transaction.acceptVal!!)
            }

            signal(Signal.OnHandlingAgreeBegin, transaction, message.change)

            if (message.ballotNumber < myBallotNumber) {
                logger.info("Cannot agree, not a valid leader")
                throw NotValidLeader(myBallotNumber, message.ballotNumber)
            }

            myBallotNumber = message.ballotNumber

            if (!this@GPACProtocolImpl.transaction.decision) {
                try {
                    transactionBlocker.acquireReentrant(
                        TransactionAcquisition(ProtocolName.GPAC, message.change.id),
                    )
                } catch (e: Exception) {
                    logger.info("Failed to acquire lock", e)
                    return@withLock Agreed(
                        ballotNumber = message.ballotNumber,
                        acceptVal = Accept.ABORT,
                    )
                }
            }
            logger.info("Lock acquired for ballot ${message.ballotNumber}")

            val entry = message.change.toHistoryEntry(peersetId)
            val initVal = if (history.isEntryCompatible(entry)) Accept.COMMIT else Accept.ABORT

            transaction =
                Transaction(
                    ballotNumber = message.ballotNumber,
                    change = message.change,
                    acceptVal = message.acceptVal,
                    initVal = initVal,
                    acceptNum = message.acceptNum ?: message.ballotNumber,
                )

            logger.info("Agreeing to transaction: ${this@GPACProtocolImpl.transaction}")

            signal(Signal.OnHandlingAgreeEnd, transaction, message.change)

            if (isMetricTest) {
                Metrics.bumpChangeMetric(
                    changeId = message.change.id,
                    peerId = peerId,
                    peersetId = peersetId,
                    protocolName = ProtocolName.GPAC,
                    state = "agreed",
                )
            }

            leaderFailTimeoutStart(message.change)

            return@withLock Agreed(transaction.ballotNumber, message.acceptVal)
        }
    }

    override suspend fun handleApply(message: Apply) =
        phaseMutex.withLock {
            logger.info("HandleApply message: $message")
            val isCurrentTransaction =
                message.ballotNumber >= this@GPACProtocolImpl.myBallotNumber

            if (isCurrentTransaction) leaderFailTimeoutStop()
            signal(Signal.OnHandlingApplyBegin, transaction, message.change)

            val entry = message.change.toHistoryEntry(peersetId)

            when {
                !isCurrentTransaction && !transactionBlocker.isAcquired() -> {
                    if (!history.containsEntry(entry.getId())) {
                        transactionBlocker.acquireReentrant(
                            TransactionAcquisition(ProtocolName.GPAC, message.change.id),
                        )
                    }

                    transaction =
                        Transaction(
                            ballotNumber = message.ballotNumber,
                            change = message.change,
                            acceptVal = message.acceptVal,
                            initVal = message.acceptVal,
                            acceptNum = message.ballotNumber,
                        )
                }

                !isCurrentTransaction -> {
                    logger.info(" is not blocked")
                    changeConflicts(message.change, "Don't receive ft-agree and can't block on history")
                    throw TransactionNotBlockedOnThisChange(ProtocolName.GPAC, message.change.id)
                }
            }

            try {
                this@GPACProtocolImpl.transaction =
                    this@GPACProtocolImpl.transaction.copy(decision = true, acceptVal = Accept.COMMIT, ended = true)

                val (changeResult, resultMessage) =
                    if (message.acceptVal == Accept.COMMIT && !history.containsEntry(entry.getId())) {
                        addChangeToHistory(message.change)
                        signal(Signal.OnHandlingApplyCommitted, transaction, message.change)
                        Pair(ChangeResult.Status.SUCCESS, null)
                    } else if (message.acceptVal == Accept.ABORT) {
                        Pair(ChangeResult.Status.ABORTED, "Message was applied but state was ABORT")
                    } else {
                        Pair(ChangeResult.Status.SUCCESS, null)
                    }

                logger.info("handleApply releaseBlocker")

                changeResult.resolveChange(message.change.id, resultMessage)
                if (isMetricTest) {
                    Metrics.bumpChangeMetric(
                        changeId = message.change.id,
                        peerId = peerId,
                        peersetId = peersetId,
                        protocolName = ProtocolName.GPAC,
                        state = changeResult.name.lowercase(),
                    )
                }
            } catch (ex: Exception) {
                logger.error("Exception during applying change, set it to abort", ex)
                transaction =
                    transaction.copy(ballotNumber = myBallotNumber, decision = true, initVal = Accept.ABORT, change = null)
            } finally {
                logger.info("handleApply finally releaseBlocker")
                transactionBlocker.tryRelease(
                    TransactionAcquisition(
                        ProtocolName.GPAC,
                        message.change.id,
                    ),
                )
                signal(Signal.OnHandlingApplyEnd, transaction, message.change)
            }
        }

    private fun addChangeToHistory(change: Change) {
        change.toHistoryEntry(peersetId).let {
            history.addEntry(it)
        }
    }

    private fun changeWasAppliedBefore(change: Change) = Changes.fromHistory(history).any { it.id == change.id }

    private suspend fun leaderFailTimeoutStart(change: Change) {
        logger.debug("Start a timeout for detecting leader failure")
        leaderTimer.startCounting {
            logger.info("Recovery leader starts after a timeout")
            if (!changeWasAppliedBefore(change)) performProtocolAsRecoveryLeader(change)
        }
    }

    private fun leaderFailTimeoutStop() {
        logger.debug("Stop the timeout for detecting leader failure")
        leaderTimer.cancelCounting()
    }

    override suspend fun performProtocolAsLeader(
        change: Change,
        iteration: Int,
    ) {
        logger.info("Starting performing GPAC iteration: $iteration")
        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())

        try {
            val electMeResult =
                electMePhase(change, { responses ->
                    superSet(responses, getPeersFromChange(change)) { it.initVal == Accept.COMMIT } ||
                        superSet(responses, getPeersFromChange(change)) { it.initVal == Accept.ABORT }
                })

            if (iteration == maxLeaderElectionTries) {
                val message = "Transaction failed due to too many retries of becoming a leader."
                logger.error(message)
                signal(Signal.ReachedMaxRetries, transaction, change)
                transaction = transaction.copy(change = null)
                changeTimeout(change, message)
                return
            }

            if (!electMeResult.success) {
                retriesTimer.startCounting(iteration) {
                    performProtocolAsLeader(change, iteration + 1)
                }
                return
            }

            val electResponses = electMeResult.responses

            val acceptVal = electResponses.getAcceptVal(change)

            if (acceptVal == null) {
                retriesTimer.startCounting(iteration) {
                    performProtocolAsLeader(change, iteration + 1)
                }
                return
            }

            this@GPACProtocolImpl.transaction = this@GPACProtocolImpl.transaction.copy(acceptVal = acceptVal, acceptNum = myBallotNumber)

            applySignal(Signal.BeforeSendingAgree, this@GPACProtocolImpl.transaction, change)

            val agreed = ftAgreePhase(change, acceptVal)
            if (!agreed) {
                return
            }

            try {
                applySignal(Signal.BeforeSendingApply, this@GPACProtocolImpl.transaction, change)
            } catch (e: Exception) {
                transaction = Transaction(myBallotNumber, Accept.ABORT, change = null)
                logger.error("Exception in Signal BeforeSendingApply", e.cause)
                transactionBlocker.release(TransactionAcquisition(ProtocolName.GPAC, change.id))
                throw e
            }
            applyPhase(change, acceptVal)
        } catch (e: Exception) {
            logger.error("Error while performing protocol as leader for change ${change.id}", e)
            changeIdToCompletableFuture[change.id]!!.complete(ChangeResult(ChangeResult.Status.CONFLICT, e.message))
        }
    }

    private fun Map<PeersetId, List<ElectedYou>>.getAcceptVal(change: Change): Accept? {
        val shouldCommit = superSet(this, getPeersFromChange(change)) { it.initVal == Accept.COMMIT }
        val shouldAbort = superSet(this, getPeersFromChange(change)) { it.initVal == Accept.ABORT }

        return when {
            shouldCommit -> Accept.COMMIT
            shouldAbort -> Accept.ABORT
            else -> null
        }
    }

    override suspend fun performProtocolAsRecoveryLeader(
        change: Change,
        iteration: Int,
    ) {
        logger.info("Starting performing GPAC iteration: $iteration as recovery leader")
        changeIdToCompletableFuture.putIfAbsent(change.id, CompletableFuture())
        val electMeResult =
            electMePhase(
                change,
                { responses -> superMajority(responses, getPeersFromChange(change)) },
                this.transaction,
                this.transaction.acceptNum,
            )

        if (iteration == maxLeaderElectionTries) {
            val message = "Transaction failed due to too many retries of becoming a leader."
            logger.error(message)
            signal(Signal.ReachedMaxRetries, transaction, change)
            changeTimeout(change, message)
            transaction = transaction.copy(change = null)
            transactionBlocker.release(TransactionAcquisition(ProtocolName.GPAC, change.id))
            return
        }

        if (!electMeResult.success) {
            retriesTimer.startCounting(iteration) {
                if (!changeWasAppliedBefore(change)) {
                    performProtocolAsRecoveryLeader(change, iteration + 1)
                }
            }
            return
        }

        val electResponses = electMeResult.responses

        val messageWithDecision = electResponses.values.flatten().find { it.decision }
        if (messageWithDecision != null) {
            logger.info("Got hit with message with decision true")
            // someone got to ft-agree phase
            this.transaction = this.transaction.copy(acceptVal = messageWithDecision.acceptVal)
            signal(Signal.BeforeSendingAgree, this.transaction, change)

            val agreed =
                ftAgreePhase(
                    change,
                    messageWithDecision.acceptVal!!,
                    decision = messageWithDecision.decision,
                    acceptNum = this.transaction.acceptNum,
                )
            if (!agreed) {
                return
            }

            signal(Signal.BeforeSendingApply, this.transaction, change)
            applyPhase(change, messageWithDecision.acceptVal)

            return
        }

        // I got to ft-agree phase, so my voice of this is crucial
        signal(Signal.BeforeSendingAgree, this.transaction, change)

        logger.info("Recovery leader transaction state: ${this.transaction}")
        val agreed =
            ftAgreePhase(
                change,
                this.transaction.acceptVal!!,
                acceptNum = this.transaction.acceptNum,
            )
        if (!agreed) {
            return
        }

        signal(Signal.BeforeSendingApply, this.transaction, change)
        applyPhase(change, this.transaction.acceptVal!!)

        return
    }

    data class ElectMeResult(val responses: Map<PeersetId, List<ElectedYou>>, val success: Boolean)

    private suspend fun electMePhase(
        change: Change,
        superFunction: (Map<PeersetId, List<ElectedYou>>) -> Boolean,
        transaction: Transaction? = null,
        acceptNum: Int? = null,
    ): ElectMeResult {
        if (!history.isEntryCompatible(change.toHistoryEntry(peersetId))) {
            signal(Signal.OnSendingElectBuildFail, this@GPACProtocolImpl.transaction, change)
            changeRejected(
                change,
                "History entry not compatible, change: $change, expected: ${history.getCurrentEntryId()}",
            )
            throw HistoryCannotBeBuildException()
        }

        myBallotNumber++
        this@GPACProtocolImpl.transaction =
            transaction ?: Transaction(ballotNumber = myBallotNumber, initVal = Accept.COMMIT, change = change)

        signal(Signal.BeforeSendingElect, this@GPACProtocolImpl.transaction, change)
        logger.info("Sending ballot ($myBallotNumber) and waiting for election result")
        val responses = getElectedYouResponses(change, getPeersFromChange(change), acceptNum)

        val (electResponses: Map<PeersetId, List<ElectedYou>>, success: Boolean) =
            GPACResponsesContainer(responses, gpacConfig.phasesTimeouts.electTimeout).awaitForMessages {
                superFunction(it)
            }

        if (success) {
            logger.info("Election successful")
            return ElectMeResult(electResponses, true)
        }

        myBallotNumber++
        logger.info("Election unsuccessful, bumped ballot number to $myBallotNumber")

        return ElectMeResult(electResponses, false)
    }

    private suspend fun ftAgreePhase(
        change: Change,
        acceptVal: Accept,
        decision: Boolean = false,
        acceptNum: Int? = null,
        iteration: Int = 0,
    ): Boolean {
        val acquisition = TransactionAcquisition(ProtocolName.GPAC, change.id)
        transactionBlocker.acquireReentrant(acquisition)

        val responses = getAgreedResponses(change, getPeersFromChange(change), acceptVal, decision, acceptNum)

        val (_: Map<PeersetId, List<Agreed>>, success: Boolean) =
            GPACResponsesContainer(responses, gpacConfig.phasesTimeouts.agreeTimeout).awaitForMessages {
                superSet(
                    it,
                    getPeersFromChange(change),
                )
            }

        if (!success && iteration == gpacConfig.maxFTAgreeTries) {
            changeTimeout(change, "Transaction failed due to too few responses of ft phase.")
            transactionBlocker.release(acquisition)
            return false
        }

        if (!success) {
            delay(gpacConfig.ftAgreeRepeatDelay.toMillis())
            return ftAgreePhase(change, acceptVal, decision, acceptNum, iteration + 1)
        }

        this@GPACProtocolImpl.transaction = this@GPACProtocolImpl.transaction.copy(decision = true, acceptVal = acceptVal)
        return true
    }

    private suspend fun applyPhase(
        change: Change,
        acceptVal: Accept,
    ) {
        val applyMessages = sendApplyMessages(change, getPeersFromChange(change), acceptVal)

        val (responses, _) =
            GPACResponsesContainer(
                applyMessages,
                gpacConfig.phasesTimeouts.applyTimeout,
            ).awaitForMessages {
                superSet(it, getPeersFromChange(change))
            }

        logger.info("Responses from apply: $responses")
        this@GPACProtocolImpl.handleApply(
            Apply(
                myBallotNumber,
                this@GPACProtocolImpl.transaction.decision,
                acceptVal,
                change,
            ),
        )
    }

    private suspend fun getElectedYouResponses(
        change: Change,
        otherPeers: Map<PeersetId, List<PeerAddress>>,
        acceptNum: Int? = null,
    ): Map<PeersetId, List<Deferred<ElectedYou?>>> =
        protocolClient.sendElectMe(
            otherPeers,
            ElectMe(myBallotNumber, change, acceptNum),
        )

    private suspend fun getAgreedResponses(
        change: Change,
        otherPeers: Map<PeersetId, List<PeerAddress>>,
        acceptVal: Accept,
        decision: Boolean = false,
        acceptNum: Int? = null,
    ): Map<PeersetId, List<Deferred<Agreed?>>> =
        protocolClient.sendFTAgree(
            otherPeers,
            Agree(myBallotNumber, acceptVal, change, decision, acceptNum),
        )

    private suspend fun sendApplyMessages(
        change: Change,
        otherPeers: Map<PeersetId, List<PeerAddress>>,
        acceptVal: Accept,
    ) = protocolClient.sendApply(
        otherPeers,
        Apply(
            myBallotNumber,
            this@GPACProtocolImpl.transaction.decision,
            acceptVal,
            change,
        ),
    )

    private fun signal(
        signal: Signal,
        transaction: Transaction?,
        change: Change,
    ) {
        signalPublisher.signal(
            signal,
            this,
            getPeersFromChange(change),
            transaction,
            change,
        )
    }

    private fun <T> superMajority(
        responses: Map<PeersetId, List<T>>,
        peers: Map<PeersetId, List<PeerAddress>>,
    ): Boolean = responses.size.isMoreThanHalfOf(peers.size) && superFunction(responses, peers)

    private fun <T> superSet(
        responses: Map<PeersetId, List<T>>,
        peers: Map<PeersetId, List<PeerAddress>>,
        condition: (T) -> Boolean = { true },
    ): Boolean {
        val allPeersetsResponded = peers.count { it.value.isNotEmpty() } == responses.count { it.value.isNotEmpty() }
        return allPeersetsResponded && superFunction(responses, peers, condition)
    }

    private fun <T> superFunction(
        responses: Map<PeersetId, List<T>>,
        peers: Map<PeersetId, List<PeerAddress>>,
        condition: (T) -> Boolean = { true },
    ): Boolean {
        return responses.all { (responsePeersetId, responses) ->
            val allPeers =
                if (responsePeersetId == peersetId) {
                    peers[responsePeersetId]!!.size + 1
                } else {
                    peers[responsePeersetId]!!.size
                }
            val agreedPeers =
                if (responsePeersetId == peersetId) {
                    responses.count { condition(it) } + 1
                } else {
                    responses.count { condition(it) }
                }
            agreedPeers >= (floor(allPeers * 0.5) + 1)
            agreedPeers.isMoreThanHalfOf(allPeers)
        }
    }

    private fun Int.isMoreThanHalfOf(otherValue: Int) = this >= (floor(otherValue * 0.5) + 1)

    private fun applySignal(
        signal: Signal,
        transaction: Transaction,
        change: Change,
    ) {
        try {
            signal(signal, transaction, change)
        } catch (e: Exception) {
            // TODO change approach to simulating errors in signal listeners
            changeTimeout(change, e.toString())
            throw e
        }
    }

    private fun changeRejected(
        change: Change,
        detailedMessage: String? = null,
    ) {
        changeIdToCompletableFuture[change.id]?.complete(ChangeResult(ChangeResult.Status.REJECTED, detailedMessage))
    }

    private fun changeConflicts(
        change: Change,
        detailedMessage: String? = null,
    ) {
        changeIdToCompletableFuture[change.id]?.complete(ChangeResult(ChangeResult.Status.CONFLICT, detailedMessage))
    }

    private fun changeTimeout(
        change: Change,
        detailedMessage: String? = null,
    ) {
        changeIdToCompletableFuture[change.id]!!.complete(ChangeResult(ChangeResult.Status.TIMEOUT, detailedMessage))
    }

    override fun getChangeResult(changeId: String): CompletableFuture<ChangeResult>? = changeIdToCompletableFuture[changeId]

    override suspend fun performProtocol(change: Change) = performProtocolAsLeader(change)

    companion object {
        private val logger = LoggerFactory.getLogger("gpac")
    }

    private fun ChangeResult.Status.resolveChange(
        changeId: String,
        detailedMessage: String? = null,
    ) {
        changeIdToCompletableFuture[changeId]?.complete(ChangeResult(this, detailedMessage))
    }
}
