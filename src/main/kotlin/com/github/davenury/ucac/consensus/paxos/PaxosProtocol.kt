package com.github.davenury.ucac.consensus.paxos

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.ucac.consensus.ConsensusProtocol
import java.util.concurrent.CompletableFuture

interface PaxosProtocol : ConsensusProtocol {
    suspend fun handlePropose(message: PaxosPropose): PaxosPromise

    suspend fun handleAccept(message: PaxosAccept): PaxosAccepted

    suspend fun handleCommit(message: PaxosCommit): PaxosCommitResponse

    suspend fun handleBatchCommit(message: PaxosBatchCommit)

    suspend fun handleProposeChange(change: Change): CompletableFuture<ChangeResult>
}
