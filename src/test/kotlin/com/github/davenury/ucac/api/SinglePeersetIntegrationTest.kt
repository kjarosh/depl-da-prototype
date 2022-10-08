package com.github.davenury.ucac.api

import com.fasterxml.jackson.module.kotlin.readValue
import com.github.davenury.ucac.*
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.common.*
import com.github.davenury.ucac.gpac.domain.*
import com.github.davenury.ucac.utils.TestApplicationSet
import io.ktor.client.features.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import kotlinx.coroutines.runBlocking
import org.apache.commons.io.FileUtils
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.fail
import org.slf4j.LoggerFactory
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*
import java.io.File
import java.time.Duration
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicBoolean

@Suppress("HttpUrlsUsage")
class SinglePeersetIntegrationTest {
    companion object {
        private val log = LoggerFactory.getLogger(SinglePeersetIntegrationTest::class.java)!!
    }

    @BeforeEach
    fun setup() {
        System.setProperty("configFile", "single_peerset_application.conf")
        deleteRaftHistories()
    }

    @Test
    fun `second leader tries to become leader before first leader goes into ft-agree phase`(): Unit = runBlocking {
        val signalExecuted = AtomicBoolean(false)

        val signalListener = SignalListener {
            expectCatching {
                executeChange("http://${it.peers[0][1]}/create_change", changeDto(listOf()))
            }.isSuccess()
            signalExecuted.set(true)
            throw RuntimeException("Stop")
        }

        val apps = TestApplicationSet(
            1, listOf(3),
            signalListeners = mapOf(1 to mapOf(Signal.BeforeSendingAgree to signalListener))
        )
        val peers = apps.getPeers()

        // Leader fails due to ballot number check - second leader bumps ballot number to 2, then ballot number of leader 1 is too low - should we handle it?
        expectThrows<ServerResponseException> {
            executeChange("http://${peers[0][0]}/create_change", changeDto(listOf()))
        }

        apps.stopApps()
    }

    @Test
    fun `first leader is already in ft-agree phase and second leader tries to execute its transaction - second should be rejected`(): Unit =
        runBlocking {

            val signalListener = SignalListener {
                expectCatching {
                    executeChange("http://${it.peers[0][1]}/create_change", changeDto(listOf()))
                }.isFailure()
            }

            val apps = TestApplicationSet(1, listOf(3),
                signalListeners = mapOf(1 to mapOf(Signal.BeforeSendingApply to signalListener)),
            )
            val peers = apps.getPeers()

            expectCatching {
                executeChange("http://${peers[0][0]}/create_change", changeDto(listOf()))
            }.isSuccess()

            apps.stopApps()
        }

    @Test
    fun `should be able to execute transaction even if leader fails after first ft-agree`() {
        runBlocking {

            val phaser = Phaser(2)

            val firstLeaderAction = SignalListener { runBlocking {
                val url = "http://${it.peers[0][1]}/ft-agree"
                val response = testHttpClient.post<Agreed>(url) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = Agree(it.transaction!!.ballotNumber, Accept.COMMIT, changeDto(listOf(it.peers[0][0])))
                }
                throw RuntimeException("Stop")
            }}
            val firstLeaderCallbacks: Map<Signal, SignalListener> = mapOf(
                Signal.BeforeSendingAgree to firstLeaderAction
            )
            val afterHandlingApply = SignalListener {
                phaser.arriveAndAwaitAdvance()
            }
            val peer2Callbacks: Map<Signal, SignalListener> = mapOf(
                Signal.OnHandlingApplyEnd to afterHandlingApply
            )

            val apps = TestApplicationSet(1, listOf(3),
                signalListeners = mapOf(
                    1 to firstLeaderCallbacks,
                    2 to peer2Callbacks
                ),
                configOverrides = mapOf(2 to mapOf("protocol.leaderFailTimeout" to Duration.ZERO))
            )
            val peers = apps.getPeers()

            // change that will cause leader to fall according to action
            try {
                executeChange("http://${peers[0][0]}/create_change", changeDto(listOf()))
                fail("Change passed")
            } catch (e: Exception) {
                log.info("Leader 1 fails: $e")
            }

            phaser.arriveAndAwaitAdvance()
            val response = testHttpClient.get<String>("http://${peers[0][2]}/change") {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }

            val change = objectMapper.readValue<ChangeWithAcceptNumDto>(response).let {
                ChangeWithAcceptNum(it.changeDto.toChange(), it.acceptNum)
            }
            expectThat(change.change.toDto().properties).isEqualTo(changeDto(listOf(peers[0][0])).properties)

            apps.stopApps()
        }
    }

    @Test
    fun `should be able to execute transaction even if leader fails after first apply`(): Unit = runBlocking {
        val phaser = Phaser(2)
        // since leader applies the change not in heartbeat
        val consensusPhaser = Phaser(5)

        val consensusPeersAction = SignalListener {
            val expectedChange = it.change
            if (expectedChange?.toDto()?.properties == mapOf("operation" to "ADD_GROUP", "groupName" to "name")) {
                consensusPhaser.arriveAndAwaitAdvance()
            }
        }
        val consensusPeerCallback = mapOf(
            Signal.ConsensusAfterHandlingHeartbeat to consensusPeersAction
        )

        val firstLeaderAction = SignalListener {
            val url = "http://${it.peers[0][1]}/apply"
            runBlocking {
                testHttpClient.post<HttpResponse>(url) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = Apply(
                        it.transaction!!.ballotNumber,
                        true,
                        Accept.COMMIT,
                        ChangeDto(mapOf("operation" to "ADD_GROUP", "groupName" to "name"), listOf("http://${it.peers[0][1]}"))
                    )
                }.also {
                    log.info("Got response ${it.status.value}")
                }
            }
            throw RuntimeException()
        }
        val firstLeaderCallbacks: Map<Signal, SignalListener> = mapOf(
            Signal.BeforeSendingApply to firstLeaderAction
        )

        val peer3Action = SignalListener {
            phaser.arriveAndAwaitAdvance()
        }
        val peer3Callbacks: Map<Signal, SignalListener> = mapOf(
            Signal.OnHandlingApplyEnd to peer3Action
        )

        val apps = TestApplicationSet(1, listOf(5),
            signalListeners = mapOf(
                1 to firstLeaderCallbacks + consensusPeerCallback,
                2 to consensusPeerCallback,
                3 to peer3Callbacks + consensusPeerCallback,
                4 to consensusPeerCallback,
                5 to consensusPeerCallback
            )
        )
        val peers = apps.getPeers()

        // change that will cause leader to fall according to action
        try {
            executeChange("http://${peers[0][0]}/create_change", ChangeDto(mapOf("operation" to "ADD_GROUP", "groupName" to "name"), listOf()))
            fail("Change passed")
        } catch (e: Exception) {
            log.info("Leader 1 fails: $e")
        }

        // leader timeout is 5 seconds for integration tests - in the meantime other peer should wake up and execute transaction
        phaser.arriveAndAwaitAdvance()
        consensusPhaser.arriveAndAwaitAdvance()

        val response = testHttpClient.get<String>("http://${peers[0][3]}/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        val change = objectMapper.readValue(response, ChangeWithAcceptNumDto::class.java)
        expectThat(change.changeDto.properties).isEqualTo(mapOf("operation" to "ADD_GROUP", "groupName" to "name"))

        // and should not execute this change couple of times
        val response2 = testHttpClient.get<String>("http://${peers[0][1]}/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        val values: List<ChangeWithAcceptNum> = objectMapper.readValue<HistoryDto>(response2).changes.map {
            ChangeWithAcceptNum(it.changeDto.toChange(), it.acceptNum)
        }
        // only one change and this change shouldn't be applied for 8082 two times
        expect {
            that(values.size).isGreaterThanOrEqualTo(1)
            that(values[0].change.toDto().properties).isEqualTo(mapOf("operation" to "ADD_GROUP", "groupName" to "name"))
        }

        apps.stopApps()
    }

    private fun createPeersInRange(range: Int): List<String> =
        List(range) { "localhost:${8081 + it}" }

    private suspend fun executeChange(uri: String, change: ChangeDto) =
        testHttpClient.post<String>(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private fun changeDto(peers: List<String>) = ChangeDto(
        mapOf(
            "operation" to "ADD_USER",
            "userName" to "userName"
        ),
        // leader should enrich himself
        peers
    )

    private fun deleteRaftHistories() {
        File(System.getProperty("user.dir")).listFiles { pathname -> pathname?.name?.startsWith("history") == true }
            ?.forEach { file -> FileUtils.deleteDirectory(file) }
    }
}