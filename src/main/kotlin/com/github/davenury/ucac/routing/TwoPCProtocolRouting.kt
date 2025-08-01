package com.github.davenury.ucac.routing

import com.github.davenury.common.Change
import com.github.davenury.common.peersetId
import com.github.davenury.ucac.commitment.twopc.TwoPC
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing

fun Application.twoPCRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.twoPC(): TwoPC {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).twoPC
    }

    routing {
        post("/protocols/2pc/accept") {
            val message = call.receive<Change>()
            call.twoPC().handleAccept(message)
            call.respond(HttpStatusCode.OK)
        }
        post("/protocols/2pc/decision") {
            val message = call.receive<Change>()
            call.twoPC().handleDecision(message)
            call.respond(HttpStatusCode.OK)
        }

        get("/protocols/2pc/ask/{changeId}") {
            val id = call.parameters["changeId"]!!
            val change = call.twoPC().getChange(id)
            call.respond(HttpStatusCode.OK, change)
        }
    }
}
