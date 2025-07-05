package com.github.davenury.ucac

import io.ktor.client.HttpClient
import io.ktor.client.engine.okhttp.OkHttp
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.serialization.jackson.jackson

val httpClient =
    HttpClient(OkHttp) {
        expectSuccess = true
        install(ContentNegotiation) {
            jackson()
        }
        install(HttpTimeout) {
            requestTimeoutMillis = 5 * 60 * 1_000
            socketTimeoutMillis = 5 * 60 * 1_000
        }
    }
val raftHttpClient =
    HttpClient(OkHttp) {
        expectSuccess = true
        install(ContentNegotiation) {
            jackson()
        }
        install(HttpTimeout) {
            requestTimeoutMillis = 750
        }
    }

val raftHttpClients =
    (0..5).map {
        HttpClient(OkHttp) {
            expectSuccess = true
            install(ContentNegotiation) {
                jackson()
            }
            install(HttpTimeout) {
                requestTimeoutMillis = 750
            }
        }
    }

val testHttpClient =
    HttpClient(OkHttp) {
        expectSuccess = true
        install(ContentNegotiation) {
            jackson()
        }
        install(HttpTimeout) {
            requestTimeoutMillis = 120000
            socketTimeoutMillis = 120000
        }
    }
