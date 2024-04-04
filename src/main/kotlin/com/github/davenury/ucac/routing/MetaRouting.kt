package com.github.davenury.ucac.routing

import com.github.davenury.common.meterRegistry
import io.ktor.application.Application
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.response.respond
import io.ktor.routing.get
import io.ktor.routing.routing

fun Application.metaRouting() {
    routing {
        get("/_meta/metrics") { call.respond(meterRegistry.scrape()) }

        get("/_meta/health") { call.respond(HttpStatusCode.OK) }
    }
}
