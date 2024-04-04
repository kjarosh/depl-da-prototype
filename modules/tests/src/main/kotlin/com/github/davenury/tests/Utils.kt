package com.github.davenury.tests

import com.github.davenury.common.objectMapper
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
            requestTimeoutMillis = 5_000
        }
    }
