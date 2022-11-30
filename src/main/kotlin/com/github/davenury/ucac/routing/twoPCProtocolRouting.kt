package com.github.davenury.ucac.routing

import com.github.davenury.common.Change
import com.github.davenury.ucac.commitment.TwoPC.TwoPC
import io.ktor.application.*
import io.ktor.http.*
import io.ktor.request.*
import io.ktor.response.*
import io.ktor.routing.*

fun Application.twoPCRouting(twoPC: TwoPC) {
    // Starting point for a Ktor app:
    routing {
        post("/2pc/accept") {
            val message = call.receive<Change>()
            twoPC.handleAccept(message)
            call.respond(HttpStatusCode.OK)
        }
        post("/2pc/decision") {
            val message = call.receive<Change>()
            twoPC.handleDecision(message)
            call.respond(HttpStatusCode.OK)
        }

        get("/2pc/ask/{changeId}") {
            val id = call.parameters["changeId"]!!
            val change = twoPC.getChange(id)
            call.respond(HttpStatusCode.OK, change)
        }
    }
}