package com.github.davenury.ucac.api

import com.fasterxml.jackson.module.kotlin.readValue
import com.github.davenury.ucac.*
import com.github.davenury.ucac.SignalListener
import com.github.davenury.ucac.common.AddGroupChange
import com.github.davenury.ucac.common.AddUserChange
import com.github.davenury.ucac.common.ChangeDto
import com.github.davenury.ucac.consensus.ratis.ChangeWithAcceptNum
import com.github.davenury.ucac.consensus.ratis.ChangeWithAcceptNumDto
import com.github.davenury.ucac.consensus.ratis.HistoryDto
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
import strikt.api.expect
import strikt.api.expectCatching
import strikt.api.expectThat
import strikt.api.expectThrows
import strikt.assertions.*
import java.io.File
import java.util.*
import kotlin.random.Random
import java.time.Duration
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicBoolean

class SinglePeersetIntegrationTest {

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
                executeChange("http://${it.otherPeers[0][0]}/create_change")
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
            executeChange("http://${peers[0][0]}/create_change")
        }

        apps.stopApps()
    }

    @Test
    fun `first leader is already in ft-agree phase and second leader tries to execute its transaction - second should be rejected`(): Unit =
        runBlocking {
            val numberOfApps = 3
            val ratisPorts = List(numberOfApps) { Random.nextInt(10000, 20000) + it }
            val configOverrides = mapOf("raft.server.addresses" to listOf(ratisPorts.map { "localhost:${it + 11124}" }),
                "raft.clusterGroupIds" to listOf(UUID.randomUUID()))

            val signalListener = SignalListener {
                expectCatching {
                    executeChange("http://${it.otherPeers[0][0]}/create_change")
                }.isFailure()
            }

            val apps = listOf(
                createApplication(
                    arrayOf("1", "1"),
                    signalListeners = mapOf(Signal.BeforeSendingApply to signalListener),
                    configOverrides = configOverrides,
                    mode = TestApplicationMode(1, 1),
                ),
                createApplication(
                    arrayOf("2", "1"),
                    configOverrides = configOverrides,
                    mode = TestApplicationMode(2, 1),
                ),
                createApplication(
                    arrayOf("3", "1"),
                    configOverrides = configOverrides,
                    mode = TestApplicationMode(3, 1),
                ),
            )
            apps.forEach { app -> app.startNonblocking() }
            val peers = apps.map { "localhost:${it.getBoundPort()}" }
            apps.forEachIndexed { index, app ->
                app.setOtherPeers(listOf(peers - peers[index]))
            }

            expectCatching {
                executeChange("http://${peers[0]}/create_change")
            }.isSuccess()

            apps.forEach { app -> app.stop() }
        }

    @Test
    fun `should be able to execute transaction even if leader fails after first ft-agree`() {
        runBlocking {
            val numberOfApps = 3
            val ratisPorts = List(numberOfApps) { Random.nextInt(10000, 20000) + it }
            val configOverrides = mapOf("raft.server.addresses" to listOf(ratisPorts.map { "localhost:${it + 11124}" }),
                "raft.clusterGroupIds" to listOf(UUID.randomUUID()))

            val phaser = Phaser(2)

            val firstLeaderAction = SignalListener { runBlocking {
                val url = "http://${it.otherPeers[0][0]}/ft-agree"
                val response = testHttpClient.post<Agreed>(url) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = Agree(it.transaction!!.ballotNumber, Accept.COMMIT, changeDto)
                }
                println("Other peer sent response to ft-agree: $response")
                // kill app
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
            val peer2ConfigOverrides: MutableMap<String, Any> = HashMap(configOverrides)
            peer2ConfigOverrides["protocol.leaderFailTimeout"] = Duration.ZERO
            val apps = listOf(
                createApplication(arrayOf("1", "1"), firstLeaderCallbacks, configOverrides = configOverrides, mode = TestApplicationMode(1, 1)),
                createApplication(arrayOf("2", "1"), peer2Callbacks, configOverrides = peer2ConfigOverrides, mode = TestApplicationMode(2, 1)),
                createApplication(arrayOf("3", "1"), emptyMap(), configOverrides = configOverrides, mode = TestApplicationMode(3, 1)),
            )
            apps.forEach { app -> app.startNonblocking() }
            val peers = apps.map { "localhost:${it.getBoundPort()}" }
            apps.forEachIndexed { index, app ->
                app.setOtherPeers(listOf(peers - peers[index]))
            }

            // change that will cause leader to fall according to action
            try {
                executeChange("http://${peers[0]}/create_change")
                fail("Didn't work")
            } catch (e: Exception) {
                println("Leader 1 fails: $e")
            }

            phaser.arriveAndAwaitAdvance()

            val response = testHttpClient.get<String>("http://${peers[2]}/change") {
                contentType(ContentType.Application.Json)
                accept(ContentType.Application.Json)
            }

            val change = objectMapper.readValue<ChangeWithAcceptNumDto>(response).let {
                ChangeWithAcceptNum(it.change.toChange(), it.acceptNum)
            }
            expectThat(change.change).isEqualTo(AddUserChange("userName"))

            apps.forEach { app -> app.stop() }
        }
    }

    @Test
    fun `should be able to execute transaction even if leader fails after first apply`(): Unit = runBlocking {
        val numberOfApps = 5
        val ratisPorts = List(numberOfApps) { Random.nextInt(10000, 20000) + it }
        val phaser = Phaser(2)

        val configOverrides = mapOf<String, Any>(
            "peers.peersAddresses" to listOf(createPeersInRange(numberOfApps)),
            "raft.server.addresses" to listOf(ratisPorts.map { "localhost:${it + 11124}" }),
            "raft.clusterGroupIds" to listOf(UUID.randomUUID())
        )

        val firstLeaderAction = SignalListener {
            val url = "http://${it.otherPeers[0][0]}/apply"
            runBlocking {
                testHttpClient.post<HttpResponse>(url) {
                    contentType(ContentType.Application.Json)
                    accept(ContentType.Application.Json)
                    body = Apply(
                        it.transaction!!.ballotNumber,
                        true,
                        Accept.COMMIT,
                        ChangeDto(mapOf("operation" to "ADD_GROUP", "groupName" to "name"))
                    )
                }.also {
                    println("Got response ${it.status.value}")
                }
            }
            println("${it.otherPeers[0][0]} sent response to apply")
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

        val apps = listOf(
            createApplication(arrayOf("1", "1"), firstLeaderCallbacks, configOverrides = configOverrides, mode = TestApplicationMode(1, 1)),
            createApplication(arrayOf("2", "1"), configOverrides = configOverrides, mode = TestApplicationMode(2, 1)),
            createApplication(arrayOf("3", "1"), peer3Callbacks, configOverrides = configOverrides, mode = TestApplicationMode(3, 1)),
            createApplication(arrayOf("4", "1"), configOverrides = configOverrides, mode = TestApplicationMode(4, 1)),
            createApplication(arrayOf("5", "1"), configOverrides = configOverrides, mode = TestApplicationMode(5, 1)),
        )
        apps.forEach { app -> app.startNonblocking() }
        val peers = apps.map { "localhost:${it.getBoundPort()}" }
        apps.forEachIndexed { index, app ->
            app.setOtherPeers(listOf(peers - peers[index]))
        }

        // change that will cause leader to fall according to action
        try {
            executeChange("http://${peers[0]}/create_change", mapOf("operation" to "ADD_GROUP", "groupName" to "name"))
        } catch (e: Exception) {
            println("Leader 1 fails: $e")
        }

        // leader timeout is 5 seconds for integration tests - in the meantime other peer should wake up and execute transaction
        phaser.arriveAndAwaitAdvance()

        val response = testHttpClient.get<String>("http://${peers[3]}/change") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        val change = objectMapper.readValue(response, ChangeWithAcceptNumDto::class.java)
        expectThat(change.change.toChange()).isEqualTo(AddGroupChange("name"))

        // and should not execute this change couple of times
        val response2 = testHttpClient.get<String>("http://${peers[1]}/changes") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }

        val values: List<ChangeWithAcceptNum> = objectMapper.readValue<HistoryDto>(response2).changes.map {
            ChangeWithAcceptNum(it.change.toChange(), it.acceptNum)
        }
        // only one change and this change shouldn't be applied for 8082 two times
        expect {
            that(values.size).isGreaterThanOrEqualTo(1)
            that(values[0]).isEqualTo(ChangeWithAcceptNum(AddGroupChange("name"), 1))
        }

        apps.forEach { app -> app.stop() }
    }

    private fun createPeersInRange(range: Int): List<String> =
        List(range) { "localhost:${8081 + it}" }

    private suspend fun executeChange(uri: String, change: Map<String, Any> = changeDto.properties) =
        testHttpClient.post<String>(uri) {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
            body = change
        }

    private val changeDto = ChangeDto(
        mapOf(
            "operation" to "ADD_USER",
            "userName" to "userName"
        )
    )

    private fun deleteRaftHistories() {
        File(System.getProperty("user.dir")).listFiles { pathname -> pathname?.name?.startsWith("history") == true }
            ?.forEach { file -> FileUtils.deleteDirectory(file) }
    }
}
