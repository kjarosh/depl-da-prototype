package com.github.davenury.ucac

import com.github.davenury.common.AlreadyLockedException
import com.github.davenury.common.AlvinOutdatedPrepareException
import com.github.davenury.common.ChangeDoesntExist
import com.github.davenury.common.CurrentLeaderFullInfoDto
import com.github.davenury.common.ErrorMessage
import com.github.davenury.common.GPACInstanceNotFoundException
import com.github.davenury.common.HistoryCannotBeBuildException
import com.github.davenury.common.ImNotLeaderException
import com.github.davenury.common.MissingParameterException
import com.github.davenury.common.MissingPeersetParameterException
import com.github.davenury.common.NotElectingYou
import com.github.davenury.common.NotValidLeader
import com.github.davenury.common.PeerAddress
import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.common.TwoPCHandleException
import com.github.davenury.common.UnknownOperationException
import com.github.davenury.common.UnknownPeersetException
import com.github.davenury.common.loadConfig
import com.github.davenury.common.meterRegistry
import com.github.davenury.common.objectMapper
import com.github.davenury.ucac.api.ApiV2Service
import com.github.davenury.ucac.api.apiV2Routing
import com.github.davenury.ucac.common.ChangeNotifier
import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.davenury.ucac.common.structure.Subscribers
import com.github.davenury.ucac.history.historyRouting
import com.github.davenury.ucac.routing.alvinProtocolRouting
import com.github.davenury.ucac.routing.gpacProtocolRouting
import com.github.davenury.ucac.routing.metaRouting
import com.github.davenury.ucac.routing.oldRaftProtocolRouting
import com.github.davenury.ucac.routing.pigPaxosProtocolRouting
import com.github.davenury.ucac.routing.raftProtocolRouting
import com.github.davenury.ucac.routing.twoPCRouting
import com.zopa.ktor.opentracing.OpenTracingServer
import com.zopa.ktor.opentracing.ThreadContextElementScopeManager
import io.jaegertracing.Configuration
import io.jaegertracing.internal.samplers.ConstSampler
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.application.log
import io.ktor.features.CallLogging
import io.ktor.features.ContentNegotiation
import io.ktor.features.StatusPages
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.jackson.JacksonConverter
import io.ktor.metrics.micrometer.MicrometerMetrics
import io.ktor.request.path
import io.ktor.response.respond
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.netty.NettyApplicationEngine
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Tags
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics
import io.micrometer.core.instrument.binder.system.ProcessorMetrics
import io.micrometer.core.instrument.config.MeterFilter
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.opentracing.util.GlobalTracer
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import org.slf4j.event.Level
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.jvm.isAccessible

fun main(args: Array<String>) {
    val configPrefix = "config_"
    val configOverrides = HashMap<String, String>()
    configOverrides.putAll(
        System.getenv()
            .filterKeys { it.startsWith(configPrefix) }
            .mapKeys { it.key.replaceFirst(configPrefix, "") }
            .mapKeys { it.key.replace("_", ".") },
    )

    ApplicationUcac.logger.info("Using overrides: $configOverrides")

    val application = createApplication(configOverrides = configOverrides)
    application.startNonblocking()

    while (!Thread.interrupted()) {
        try {
            Thread.sleep(1000)
        } catch (e: InterruptedException) {
            break
        }
    }

    application.stop(5000, 5000)
}

fun createApplication(
    signalListeners: Map<Signal, SignalListener> = emptyMap(),
    configOverrides: Map<String, Any> = emptyMap(),
    subscribers: Map<PeersetId, Subscribers> = emptyMap(),
): ApplicationUcac {
    val config = loadConfig<Config>(configOverrides)
    return ApplicationUcac(signalListeners, config, subscribers)
}

class ApplicationUcac(
    private val signalListeners: Map<Signal, SignalListener> = emptyMap(),
    private val config: Config,
    private val subscribers: Map<PeersetId, Subscribers>,
) {
    private val mdc: MutableMap<String, String> = HashMap(mapOf("peer" to config.peerId().toString()))
    private val peerResolver = config.newPeerResolver()
    private val engine: NettyApplicationEngine
    private lateinit var service: ApiV2Service
    private lateinit var multiplePeersetProtocols: MultiplePeersetProtocols

    init {
        MDC.getCopyOfContextMap()?.let { mdc.putAll(it) }
    }

    private fun <R> withMdc(action: () -> R): R {
        val oldMdc = MDC.getCopyOfContextMap() ?: HashMap()
        mdc.forEach { MDC.put(it.key, it.value) }
        try {
            return action()
        } finally {
            MDC.setContextMap(oldMdc)
        }
    }

    init {
        var engine: NettyApplicationEngine? = null
        withMdc {
            logger.info("Starting application with config: $config")
            engine = createServer()
        }
        config.experimentId?.let { experimentId ->
            meterRegistry.config().meterFilter(
                object : MeterFilter {
                    override fun map(id: Meter.Id): Meter.Id {
                        val tags = Tags.of(id.tags.toMutableList().also { it.add(Tag.of("experiment", experimentId)) })
                        return Meter.Id(id.name, tags, id.baseUnit, id.description, id.type)
                    }
                },
            )
        }
        this.engine = engine!!
    }

    private fun createServer() =
        embeddedServer(Netty, port = config.port, host = "0.0.0.0") {
            val peersetIds = config.peersetIds()

            logger.info("My peersets: $peersetIds")
            val changeNotifier = ChangeNotifier(peerResolver)

            if (config.configureTraces) {
                val tracer =
                    Configuration("${peerResolver.peerName()}-${config.experimentId}")
                        .withSampler(
                            Configuration.SamplerConfiguration.fromEnv()
                                .withType(ConstSampler.TYPE)
                                .withParam(1),
                        )
                        .withReporter(
                            Configuration.ReporterConfiguration.fromEnv()
                                .withLogSpans(false)
                                .withSender(
                                    Configuration.SenderConfiguration()
                                        .withAgentHost(System.getenv("TEMPO_HOST"))
                                        .withAgentPort(6831),
                                ),
                        ).tracerBuilder
                        .withScopeManager(ThreadContextElementScopeManager())
                        .build()
                GlobalTracer.registerIfAbsent(tracer)
            }

            val signalPublisher = SignalPublisher(signalListeners, peerResolver)

            multiplePeersetProtocols =
                MultiplePeersetProtocols(
                    config,
                    peerResolver,
                    signalPublisher,
                    changeNotifier,
                    subscribers,
                )

            service = ApiV2Service(config, multiplePeersetProtocols, changeNotifier, peerResolver)

            install(OpenTracingServer) {
                addTag("threadName") { Thread.currentThread().name }
                config.experimentId?.let { addTag("experiment") { it } }
                filter {
                        call ->
                    call.request.path().startsWith("/_meta") || call.request.path().startsWith("/consensus/heartbeat")
                }
            }

            install(CallLogging) {
                level = Level.DEBUG
                mdc.forEach { mdcEntry ->
                    mdc(mdcEntry.key) { mdcEntry.value }
                }
                mdc("peerset") {
                    val peerset = it.request.queryParameters["peerset"]
                    if (peerset != null && peersetIds.contains(PeersetId(peerset))) {
                        peerset
                    } else {
                        "invalid:$peerset"
                    }
                }
            }

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

            install(StatusPages) {
                exception<MissingParameterException> { cause ->
                    call.respond(
                        status = HttpStatusCode.UnprocessableEntity,
                        ErrorMessage("Missing parameter: ${cause.message}"),
                    )
                }
                exception<UnknownOperationException> { cause ->
                    call.respond(
                        status = HttpStatusCode.UnprocessableEntity,
                        ErrorMessage(
                            "Unknown operation to perform: ${cause.desiredOperationName}",
                        ),
                    )
                }
                exception<NotElectingYou> { cause ->
                    call.respond(
                        status = HttpStatusCode.UnprocessableEntity,
                        ErrorMessage(
                            "You're not valid leader-to-be. My Ballot Number is: ${cause.ballotNumber}, whereas provided was ${cause.messageBallotNumber}",
                        ),
                    )
                }
                exception<NotValidLeader> { cause ->
                    call.respond(
                        status = HttpStatusCode.UnprocessableEntity,
                        ErrorMessage(
                            "You're not valid leader. My Ballot Number is: ${cause.ballotNumber}, whereas provided was ${cause.messageBallotNumber}",
                        ),
                    )
                }
                exception<HistoryCannotBeBuildException> {
                    call.respond(
                        HttpStatusCode.BadRequest,
                        ErrorMessage(
                            "Change you're trying to perform is not applicable with current state",
                        ),
                    )
                }
                exception<AlreadyLockedException> { cause ->
                    call.respond(
                        HttpStatusCode.Conflict,
                        ErrorMessage(
                            cause.message!!,
                        ),
                    )
                }

                exception<ChangeDoesntExist> { cause ->
                    logger.error("Change doesn't exist", cause)
                    call.respond(
                        status = HttpStatusCode.NotFound,
                        ErrorMessage(cause.message ?: "Change doesn't exists"),
                    )
                }

                exception<TwoPCHandleException> { cause ->
                    call.respond(
                        status = HttpStatusCode.BadRequest,
                        ErrorMessage(
                            cause.message!!,
                        ),
                    )
                }

                exception<GPACInstanceNotFoundException> { cause ->
                    logger.error("GPAC instance not found", cause)
                    call.respond(
                        status = HttpStatusCode.NotFound,
                        ErrorMessage(cause.message ?: "GPAC instance not found"),
                    )
                }

                exception<AlvinOutdatedPrepareException> { cause ->
                    call.respond(
                        status = HttpStatusCode.Unauthorized,
                        ErrorMessage(cause.message!!),
                    )
                }

                exception<MissingPeersetParameterException> { cause ->
                    logger.error("Missing peerset parameter", cause)
                    call.respond(
                        status = HttpStatusCode.BadRequest,
                        ErrorMessage("Missing peerset parameter"),
                    )
                }

                exception<UnknownPeersetException> { cause ->
                    logger.error("Unknown peerset: {}", cause.peersetId, cause)
                    call.respond(
                        status = HttpStatusCode.BadRequest,
                        ErrorMessage("Unknown peerset: ${cause.peersetId}"),
                    )
                }

                exception<ImNotLeaderException> { cause ->
                    call.respond(
                        status = HttpStatusCode.TemporaryRedirect,
                        CurrentLeaderFullInfoDto(cause.peerId, cause.peersetId),
                    )
                }

                exception<Throwable> { cause ->
                    log.error("Throwable has been thrown in Application: ", cause)
                    call.respond(
                        status = HttpStatusCode.InternalServerError,
                        ErrorMessage("UnexpectedError, $cause"),
                    )
                }
            }

            metaRouting()
            historyRouting(multiplePeersetProtocols)
            apiV2Routing(service)
            gpacProtocolRouting(multiplePeersetProtocols)
            when (config.consensus.name) {
                "raft" -> raftProtocolRouting(multiplePeersetProtocols)
                "oldRaft" -> oldRaftProtocolRouting(multiplePeersetProtocols)
                "alvin" -> alvinProtocolRouting(multiplePeersetProtocols)
                "paxos" -> pigPaxosProtocolRouting(multiplePeersetProtocols)
                else -> throw IllegalStateException("Unknown consensus type ${config.consensus.name}")
            }
            twoPCRouting(multiplePeersetProtocols)

            runBlocking {
                if (config.consensus.isEnabled) {
                    multiplePeersetProtocols.protocols.values.forEach { protocols ->
                        protocols.consensusProtocol.begin()
                    }
                }
            }
        }

    fun setPeerAddresses(peerAddresses: Map<PeerId, PeerAddress>) =
        withMdc {
            peerAddresses.forEach { (peerId, address) ->
                val oldAddress = peerResolver.resolve(peerId)
                peerResolver.setPeerAddress(peerId, address)
                val newAddress = peerResolver.resolve(peerId)
                logger.debug("Updated peer address $peerId $oldAddress -> $newAddress")
            }
        }

    fun startNonblocking() {
        withMdc {
            engine.start(wait = false)
        }
    }

    fun stop(
        gracePeriodMillis: Long = 200,
        timeoutMillis: Long = 1000,
    ) {
        withMdc {
            logger.info("Stop app ${peerResolver.currentPeer()}")
            if (this::multiplePeersetProtocols.isInitialized) {
                multiplePeersetProtocols.close()
            }
            engine.stop(gracePeriodMillis, timeoutMillis)
        }
    }

    fun getBoundPort(): Int {
        val channelsProperty =
            NettyApplicationEngine::class.declaredMemberProperties.single { it.name == "channels" }
        val oldAccessible = channelsProperty.isAccessible
        try {
            channelsProperty.isAccessible = true
            val channels = channelsProperty.get(engine) as List<*>
            val channel = channels.single() as NioServerSocketChannel
            return channel.localAddress()!!.port
        } finally {
            channelsProperty.isAccessible = oldAccessible
        }
    }

    fun getPeerId(): PeerId = config.peerId()

    fun getPeersetProtocols(peersetId: PeersetId) = multiplePeersetProtocols.forPeerset(peersetId)

    companion object {
        val logger: Logger = LoggerFactory.getLogger("ucac")
    }
}
