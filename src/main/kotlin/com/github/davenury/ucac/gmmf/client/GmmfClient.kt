package com.github.davenury.ucac.gmmf.client

import com.github.davenury.common.PeerAddress
import com.github.davenury.ucac.gmmf.routing.EffectivePermissionsMessage
import com.github.davenury.ucac.gmmf.routing.MembersMessage
import com.github.davenury.ucac.gmmf.routing.ReachesMessage
import com.github.davenury.ucac.httpClient
import com.github.kjarosh.agh.pp.graph.model.VertexId
import io.ktor.client.request.accept
import io.ktor.client.request.post
import io.ktor.http.ContentType
import io.ktor.http.contentType

class GmmfClient(peer: PeerAddress) {
    private val urlBase: String = "http://${peer.address}"

    suspend fun naiveReaches(
        from: VertexId,
        to: VertexId,
        ttl: Int,
    ): ReachesMessage {
        return httpClient.post<ReachesMessage>("$urlBase/gmmf/naive/reaches?from=$from&to=$to&ttl=$ttl") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
    }

    suspend fun naiveMembers(
        of: VertexId,
        ttl: Int,
    ): MembersMessage {
        return httpClient.post<MembersMessage>("$urlBase/gmmf/naive/members?of=$of&ttl=$ttl") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
    }

    suspend fun naiveEffectivePermissions(
        from: VertexId,
        to: VertexId,
        ttl: Int,
    ): EffectivePermissionsMessage {
        return httpClient.post<EffectivePermissionsMessage>("$urlBase/gmmf/naive/effective_permissions?from=$from&to=$to&ttl=$ttl") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
    }

    suspend fun indexedReaches(
        from: VertexId,
        to: VertexId,
    ): ReachesMessage {
        return httpClient.post<ReachesMessage>("$urlBase/gmmf/indexed/reaches?from=$from&to=$to") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
    }

    suspend fun indexedMembers(of: VertexId): MembersMessage {
        return httpClient.post<MembersMessage>("$urlBase/gmmf/indexed/members?of=$of") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
    }

    suspend fun indexedEffectivePermissions(
        from: VertexId,
        to: VertexId,
    ): EffectivePermissionsMessage {
        return httpClient.post<EffectivePermissionsMessage>("$urlBase/gmmf/indexed/effective_permissions?from=$from&to=$to") {
            contentType(ContentType.Application.Json)
            accept(ContentType.Application.Json)
        }
    }
}
