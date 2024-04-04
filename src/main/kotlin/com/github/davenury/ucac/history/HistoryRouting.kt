package com.github.davenury.ucac.history

import com.github.davenury.common.Change
import com.github.davenury.common.Changes
import com.github.davenury.common.ErrorMessage
import com.github.davenury.common.history.History
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.common.peersetId
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import io.ktor.application.Application
import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.application.log
import io.ktor.http.HttpStatusCode
import io.ktor.response.respond
import io.ktor.routing.get
import io.ktor.routing.route
import io.ktor.routing.routing

fun Application.historyRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    fun ApplicationCall.history(): History {
        return multiplePeersetProtocols.forPeerset(this.peersetId()).history
    }

    routing {
        route("/change") {
            get {
                call.history().getCurrentEntry()
                    .takeIf { it != InitialHistoryEntry }
                    ?.let { Change.fromHistoryEntry(it) }
                    ?.let { call.respond(it) }
                    ?: call.respond(
                        HttpStatusCode.NotFound,
                        ErrorMessage("No change exists"),
                    )
            }
        }
        route("/changes") {
            get {
                val changes = Changes.fromHistory(call.history())
                log.info("Changes: $changes")
                call.respond(changes)
            }
        }
    }
}
