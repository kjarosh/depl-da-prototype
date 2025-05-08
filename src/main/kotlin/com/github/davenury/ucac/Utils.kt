package com.github.davenury.ucac

import com.github.davenury.common.objectMapper
import com.zopa.ktor.opentracing.OpenTracingClient
import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.features.HttpTimeout
import io.ktor.client.features.json.JacksonSerializer
import io.ktor.client.features.json.JsonFeature

val httpClient =
    HttpClient(OkHttp) {
        install(JsonFeature) {
            serializer = JacksonSerializer(objectMapper)
        }
        install(HttpTimeout) {
            requestTimeoutMillis = 5 * 60 * 1_000
            socketTimeoutMillis = 5 * 60 * 1_000
        }
        install(OpenTracingClient)
    }
val raftHttpClient =
    HttpClient(OkHttp) {
        install(JsonFeature) {
            serializer = JacksonSerializer(objectMapper)
        }
        install(HttpTimeout) {
            requestTimeoutMillis = 750
        }
        install(OpenTracingClient)
    }

val raftHttpClients =
    (0..5).map {
        HttpClient(OkHttp) {
            install(JsonFeature) {
                serializer = JacksonSerializer(objectMapper)
            }
            install(HttpTimeout) {
                requestTimeoutMillis = 750
            }
            install(OpenTracingClient)
        }
    }

val testHttpClient =
    HttpClient(OkHttp) {
        install(JsonFeature) {
            serializer = JacksonSerializer(objectMapper)
        }
        install(HttpTimeout) {
            requestTimeoutMillis = 120000
            socketTimeoutMillis = 120000
        }
    }
