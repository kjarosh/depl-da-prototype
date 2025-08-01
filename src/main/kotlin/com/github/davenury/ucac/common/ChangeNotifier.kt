package com.github.davenury.ucac.common

import com.github.davenury.common.Change
import com.github.davenury.common.ChangeResult
import com.github.davenury.common.Notification
import com.github.davenury.common.meterRegistry
import com.github.davenury.ucac.httpClient
import io.ktor.client.call.body
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpStatement
import io.ktor.http.ContentType
import io.ktor.http.contentType
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class ChangeNotifier(
    private val peerResolver: PeerResolver,
) {
    private val executor: ExecutorService = Executors.newCachedThreadPool()

    fun notify(
        change: Change,
        changeResult: ChangeResult,
    ) {
        change.notificationUrl?.let { notificationUrl ->
            executor.submit {
                meterRegistry.timer("change_notifier_send_time").record<Unit> {
                    runBlocking {
                        sendNotification(notificationUrl, change, changeResult)
                    }
                }
            }
        }
    }

    private suspend fun sendNotification(
        notificationUrl: String,
        change: Change,
        changeResult: ChangeResult,
    ) {
        try {
            val response =
                httpClient.post(notificationUrl) {
                    contentType(ContentType.Application.Json)
                    setBody(Notification(change, changeResult, sender = peerResolver.currentPeerAddress()))
                }.body<HttpStatement>()
            logger.info("Response from notifier: ${response.execute().status.value}")
        } catch (e: Exception) {
            logger.error("Error while sending notification to $notificationUrl", e)
            meterRegistry.counter("change_notifier_failed").increment()
        }
    }

    private val logger = LoggerFactory.getLogger("ChangeNotifier")
}
