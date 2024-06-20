package com.github.davenury.ucac.routing

import com.github.davenury.common.peersetId
import com.github.davenury.ucac.commitment.gpac.Agree
import com.github.davenury.ucac.commitment.gpac.Apply
import com.github.davenury.ucac.commitment.gpac.ElectMe
import com.github.davenury.ucac.commitment.gpac.GPACFactory
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import io.ktor.application.Application
import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.routing.post
import io.ktor.routing.routing

fun Application.gpacProtocolRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.gpac(): GPACFactory {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).gpacFactory
    }

    routing {
        post("/protocols/gpac/elect") {
            val message = call.receive<ElectMe>()
            call.respond(call.gpac().handleElect(message))
        }

        post("/protocols/gpac/ft-agree") {
            val message = call.receive<Agree>()
            call.respond(call.gpac().handleAgree(message))
        }

        post("/protocols/gpac/apply") {
            val message = call.receive<Apply>()
            call.gpac().handleApply(message)
            call.respond(HttpStatusCode.OK)
        }
    }
}
