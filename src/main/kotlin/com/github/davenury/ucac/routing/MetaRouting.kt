package com.github.davenury.ucac.routing

import com.github.davenury.common.meterRegistry
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.call
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.routing

fun Application.metaRouting() {
    routing {
        get("/_meta/metrics") { call.respond(meterRegistry.scrape()) }

        get("/_meta/health") { call.respond(HttpStatusCode.OK) }
    }
}
