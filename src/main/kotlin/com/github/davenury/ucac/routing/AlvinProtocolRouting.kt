package com.github.davenury.ucac.routing

import com.github.davenury.common.Changes
import com.github.davenury.common.peersetId
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.davenury.ucac.consensus.alvin.AlvinAccept
import com.github.davenury.ucac.consensus.alvin.AlvinCommit
import com.github.davenury.ucac.consensus.alvin.AlvinFastRecovery
import com.github.davenury.ucac.consensus.alvin.AlvinPropose
import com.github.davenury.ucac.consensus.alvin.AlvinProtocol
import com.github.davenury.ucac.consensus.alvin.AlvinStable
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing

fun Application.alvinProtocolRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.consensus(): AlvinProtocol {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).consensusProtocol as AlvinProtocol
    }
    routing {
        post("/protocols/alvin/proposal") {
            val message: AlvinPropose = call.receive()
            val response = call.consensus().handleProposalPhase(message)
            call.respond(response)
        }

        post("/protocols/alvin/accept") {
            val message: AlvinAccept = call.receive()
            val response = call.consensus().handleAcceptPhase(message)
            call.respond(response)
        }

        post("/protocols/alvin/stable") {
            val message: AlvinStable = call.receive()
            val result = call.consensus().handleStable(message)
            call.respond(result)
        }

        post("/protocols/alvin/prepare") {
            val message: AlvinAccept = call.receive()
            val result = call.consensus().handlePrepare(message)
            call.respond(result)
        }

        post("/protocols/alvin/commit") {
            val message: AlvinCommit = call.receive()
            val result = call.consensus().handleCommit(message)
            call.respond(HttpStatusCode.OK, result)
        }

        post("/protocols/alvin/fast-recovery") {
            val message: AlvinFastRecovery = call.receive()
            val result = call.consensus().handleFastRecovery(message)
            call.respond(result)
        }

        get("/protocols/alvin/proposed_changes") {
            call.respond(Changes(call.consensus().getProposedChanges()))
        }
        get("/protocols/alvin/accepted_changes") {
            call.respond(Changes(call.consensus().getAcceptedChanges()))
        }
    }
}
