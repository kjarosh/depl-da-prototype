package com.github.davenury.ucac.api

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeCreationResponse
import com.github.davenury.common.ChangeCreationStatus
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.PeersetId
import com.github.davenury.common.ProtocolName
import com.github.davenury.common.SubscriberAddress
import com.github.davenury.common.peersetId
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.call
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.CompletableFuture

fun Application.apiV2Routing(service: ApiV2Service) {
    val logger = LoggerFactory.getLogger("V2Routing")

    suspend fun respondChangeResult(
        result: ChangeResult?,
        call: ApplicationCall,
    ) {
        when (result?.status) {
            ChangeResult.Status.SUCCESS -> {
                call.respond(
                    HttpStatusCode.Created,
                    ChangeCreationResponse(
                        "Change applied",
                        detailedMessage = result.detailedMessage,
                        changeStatus = ChangeCreationStatus.APPLIED,
                        entryId = result.entryId,
                    ),
                )
            }

            ChangeResult.Status.CONFLICT -> {
                call.respond(
                    HttpStatusCode.Conflict,
                    ChangeCreationResponse(
                        "Change conflicted",
                        detailedMessage = result.detailedMessage,
                        changeStatus = ChangeCreationStatus.NOT_APPLIED,
                        entryId = result.entryId,
                    ),
                )
            }

            ChangeResult.Status.TIMEOUT -> {
                call.respond(
                    HttpStatusCode.InternalServerError,
                    ChangeCreationResponse(
                        "Change not applied due to timeout",
                        detailedMessage = result.detailedMessage,
                        changeStatus = ChangeCreationStatus.NOT_APPLIED,
                        entryId = result.entryId,
                    ),
                )
            }

            ChangeResult.Status.REJECTED -> {
                call.respond(
                    HttpStatusCode.InternalServerError,
                    ChangeCreationResponse(
                        "Change was rejected",
                        detailedMessage = result.detailedMessage,
                        changeStatus = ChangeCreationStatus.NOT_APPLIED,
                        entryId = result.entryId,
                    ),
                )
            }

            ChangeResult.Status.ABORTED -> {
                call.respond(
                    HttpStatusCode.InternalServerError,
                    ChangeCreationResponse(
                        "Change was applied with ABORT result (???)",
                        detailedMessage = null,
                        changeStatus = ChangeCreationStatus.UNKNOWN,
                        entryId = result.entryId,
                    ),
                )
            }

            null -> {
                call.respond(
                    HttpStatusCode.InternalServerError,
                    ChangeCreationResponse(
                        "Timed out while waiting for change (changeResult is null)",
                        detailedMessage = null,
                        changeStatus = ChangeCreationStatus.UNKNOWN,
                        entryId = null,
                    ),
                )
            }
        }
    }

    suspend fun createProcessorJob(
        peersetId: PeersetId,
        call: ApplicationCall,
    ): ProcessorJob {
        val enforceGpac: Boolean = call.request.queryParameters["enforce_gpac"]?.toBoolean() ?: false
        val useTwoPC: Boolean = call.request.queryParameters["use_2pc"]?.toBoolean() ?: false
        val change = call.receive<Change>()

        val isOnePeersetChange =
            when (change.peersets.size) {
                0 -> throw IllegalArgumentException("Change needs at least one peerset")
                1 -> true
                else -> false
            }

        if (change.peersets.find { it.peersetId == peersetId } == null) {
            throw IllegalArgumentException("My peerset ID is not in the change")
        }

        val protocolName =
            when {
                isOnePeersetChange && !enforceGpac -> ProtocolName.CONSENSUS
                useTwoPC -> ProtocolName.TWO_PC
                else -> ProtocolName.GPAC
            }

        logger.debug(
            "Create ProcessorJob from parameters: (isOnePeersetChange=$isOnePeersetChange, enforceGPAC: $enforceGpac, 2PC: $useTwoPC), resultType: $protocolName",
        )

        return ProcessorJob(change, CompletableFuture(), protocolName)
    }

    routing {
        post("/v2/change/async") {
            val peersetId = call.peersetId()
            val processorJob = createProcessorJob(peersetId, call)
            service.addChange(peersetId, processorJob)

            call.respond(HttpStatusCode.Accepted)
        }

        post("/v2/change/sync") {
            val peersetId = call.peersetId()
            val processorJob = createProcessorJob(peersetId, call)
            val timeout = call.request.queryParameters["timeout"]?.let { Duration.parse(it) }

            val result: ChangeResult? = service.addChangeSync(peersetId, processorJob, timeout)
            respondChangeResult(result, call)
        }

        get("/v2/change/{id}") {
            val peersetId = call.peersetId()
            val id = call.parameters["id"]!!
            val change = service.getChangeById(peersetId, id)
            return@get call.respond(change ?: HttpStatusCode.NotFound)
        }

        get("/v2/change_status/{id}") {
            val peersetId = call.peersetId()
            val id = call.parameters["id"]!!
            val result: ChangeResult? = service.getChangeStatus(peersetId, id).getNow(null)
            respondChangeResult(result, call)
        }

        get("/v2/change") {
            val peersetId = call.peersetId()
            call.respond(service.getChanges(peersetId))
        }

        get("/v2/last-change") {
            val peersetId = call.peersetId()
            call.respond(service.getLastChange(peersetId) ?: HttpStatusCode.NotFound)
        }

        post("/v2/subscribe-to-peer-configuration-changes") {
            val peersetId = call.peersetId()
            val address = call.receive<SubscriberAddress>()
            service.registerSubscriber(peersetId, address)
            call.respond(HttpStatusCode.OK)
        }

        get("/peerset-information") {
            val peersetId = call.peersetId()
            call.respond(service.getPeersetInformation(peersetId).toDto())
        }

        post("/consensus/latest-entry") {
            val entryId = call.receive<String>()
            val peersetId = call.peersetId()
            logger.info("Received question about latest entry in peerset: $peersetId")
            call.respond(service.getLatestEntryIdResponse(entryId, peersetId))
        }
    }
}
