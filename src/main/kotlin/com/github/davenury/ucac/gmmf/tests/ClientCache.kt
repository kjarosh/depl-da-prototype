package com.github.davenury.ucac.gmmf.tests

import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.gmmf.client.GmmfClient
import com.github.kjarosh.agh.pp.graph.model.ZoneId

/**
 * @author Kamil Jarosz
 */
class ClientCache(val peerResolver: PeerResolver) {
    private val clients: MutableMap<PeerId, GmmfClient> = mutableMapOf()

    fun getClient(zoneId: ZoneId): GmmfClient {
        return getClient(PeersetId(zoneId.id))
    }

    fun getClient(peersetId: PeersetId): GmmfClient {
        val peer = peerResolver.getPeerFromPeerset(peersetId)
        return clients.computeIfAbsent(peer.peerId) {
            GmmfClient(peerResolver, peer)
        }
    }

    fun getClient(peerId: PeerId): GmmfClient {
        return clients.computeIfAbsent(peerId) {
            GmmfClient(peerResolver, peerResolver.resolve(peerId))
        }
    }
}
