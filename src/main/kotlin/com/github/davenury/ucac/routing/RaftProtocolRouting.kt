package com.github.davenury.ucac.routing

import com.github.davenury.common.Changes
import com.github.davenury.common.peersetId
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.davenury.ucac.consensus.ConsensusProposeChange
import com.github.davenury.ucac.consensus.raft.ConsensusElectMe
import com.github.davenury.ucac.consensus.raft.ConsensusHeartbeat
import com.github.davenury.ucac.consensus.raft.RaftConsensusProtocol
import io.ktor.application.Application
import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.routing.get
import io.ktor.routing.post
import io.ktor.routing.routing
import kotlinx.coroutines.future.await

fun Application.raftProtocolRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.consensus(): RaftConsensusProtocol {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).consensusProtocol as RaftConsensusProtocol
    }
    routing {
        post("/raft/request_vote") {
            val message: ConsensusElectMe = call.receive()
            val response = call.consensus().handleRequestVote(message.peerId, message.term, message.lastEntryId)
            call.respond(response)
        }

        post("/raft/heartbeat") {
            val message: ConsensusHeartbeat = call.receive()
            val heartbeatResult = call.consensus().handleHeartbeat(message)
            call.respond(heartbeatResult)
        }

        post("/raft/request_apply_change") {
            val message: ConsensusProposeChange = call.receive()
            val result = call.consensus().handleProposeChange(message).await()
            call.respond(result)
        }

        get("/raft/proposed_changes") {
            call.respond(Changes(call.consensus().getProposedChanges()))
        }

        get("/raft/accepted_changes") {
            call.respond(Changes(call.consensus().getAcceptedChanges()))
        }
    }
}
