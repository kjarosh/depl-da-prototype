package com.github.davenury.ucac.history

import com.github.davenury.common.Change
import com.github.davenury.common.Changes
import com.github.davenury.common.ErrorMessage
import com.github.davenury.common.history.History
import com.github.davenury.common.history.InitialHistoryEntry
import com.github.davenury.common.logger
import com.github.davenury.common.peersetId
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.call
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.route
import io.ktor.server.routing.routing

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
                logger.info("Changes: $changes")
                call.respond(changes)
            }
        }
    }
}
