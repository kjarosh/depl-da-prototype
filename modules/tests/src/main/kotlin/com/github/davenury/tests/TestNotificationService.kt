package com.github.davenury.tests

import com.github.davenury.common.CurrentLeaderFullInfoDto
import com.github.davenury.common.Notification
import com.github.davenury.common.loadConfig
import com.github.davenury.common.meterRegistry
import com.github.davenury.common.objectMapper
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.JacksonConverter
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.engine.embeddedServer
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import io.ktor.server.netty.Netty
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.prometheus.client.exporter.PushGateway
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory

fun main() {
    val service = TestNotificationService()
    service.startService()
}

class TestNotificationService {
    private val config =
        loadConfig<Config>(decoders = listOf(StrategyDecoder(), ACProtocolDecoder(), CreatingChangeStrategyDecoder()))

    init {
        logger.info("Starting performance tests with config: $config")
    }

    private val peers = config.peerAddresses()
    private val changes =
        Changes(
            peers,
            HttpSender(config.acProtocol),
            config.getSendingStrategy(),
            config.getCreateChangeStrategy(),
            config.acProtocol.protocol,
            config.notificationServiceAddress,
            config.enforceConsensusLeader,
        )
    private val testExecutor =
        TestExecutor(
            changes,
            config,
        )

    private val server =
        embeddedServer(Netty, port = 8080, host = "0.0.0.0") {
            install(ContentNegotiation) {
                register(ContentType.Application.Json, JacksonConverter(objectMapper))
            }

            install(MicrometerMetrics) {
                registry = meterRegistry
                meterBinders =
                    listOf(
                        JvmMemoryMetrics(),
                        JvmGcMetrics(),
                        ProcessorMetrics(),
                    )
            }

            routing {
                post("/api/v1/notification") {
                    try {
                        val notification = call.receive<Notification>()
                        changes.handleNotification(notification)
                        call.respond(HttpStatusCode.OK)
                    } catch (e: Exception) {
                        logger.error("Error while handling notification", e)
                        call.respond(HttpStatusCode.ServiceUnavailable)
                    }
                }

                post("/api/v1/new-consensus-leader") {
                    logger.info("Received new consensus leader notification")
                    try {
                        val newConsensusLeaderId = call.receive<CurrentLeaderFullInfoDto>()
                        changes.newConsensusLeader(newConsensusLeaderId)
                        call.respond(HttpStatusCode.OK)
                    } catch (e: Exception) {
                        logger.error("Error while handling new consensus leader message", e)
                        call.respond(HttpStatusCode.ServiceUnavailable)
                    }
                }

                get("/_meta/metrics") {
                    call.respond(meterRegistry.scrape())
                }
            }

            GlobalScope.launch {
                delay(3000)
                testExecutor.startTest()
                delay(2000)
                closeService()
            }
        }

    fun startService() {
        server.start(wait = true)
    }

    fun closeService() {
        val pushGateway = PushGateway(config.pushGatewayAddress)
        pushGateway.pushAdd(meterRegistry.prometheusRegistry, "test_service")
        server.stop(200, 1000)
    }

    companion object {
        private val logger = LoggerFactory.getLogger("TestNotificationService")
    }
}
