package com.github.davenury.ucac.routing

import com.github.davenury.common.Changes
import com.github.davenury.common.peersetId
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.davenury.ucac.consensus.ConsensusProposeChange
import com.github.davenury.ucac.consensus.paxos.PaxosAccept
import com.github.davenury.ucac.consensus.paxos.PaxosBatchCommit
import com.github.davenury.ucac.consensus.paxos.PaxosCommit
import com.github.davenury.ucac.consensus.paxos.PaxosPropose
import com.github.davenury.ucac.consensus.paxos.PaxosProtocol
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.coroutines.future.await

fun Application.pigPaxosProtocolRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.consensus(): PaxosProtocol {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).consensusProtocol as PaxosProtocol
    }
    routing {
        post("/protocols/paxos/propose") {
            val message: PaxosPropose = call.receive()
            val response = call.consensus().handlePropose(message)
            call.respond(response)
        }

        post("/protocols/paxos/accept") {
            val message: PaxosAccept = call.receive()
            val acceptResult = call.consensus().handleAccept(message)
            call.respond(acceptResult)
        }

        post("/protocols/paxos/commit") {
            val message: PaxosCommit = call.receive()
            val commitResult = call.consensus().handleCommit(message)
            call.respond(commitResult)
        }

        post("/protocols/paxos/batch-commit") {
            val message: PaxosBatchCommit = call.receive()
            val batchCommitResult = call.consensus().handleBatchCommit(message)
            call.respond(batchCommitResult)
        }

        post("/protocols/paxos/request_apply_change") {
            val message: ConsensusProposeChange = call.receive()
            val result = call.consensus().handleProposeChange(message).await()
            call.respond(result)
        }

        get("/protocols/paxos/proposed_changes") {
            call.respond(Changes(call.consensus().getProposedChanges()))
        }

        get("/protocols/paxos/accepted_changes") {
            call.respond(Changes(call.consensus().getAcceptedChanges()))
        }
    }
}
