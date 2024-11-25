package com.github.davenury.ucac.gmmf.routing

import com.github.davenury.ucac.common.MultiplePeersetProtocols
import com.github.davenury.ucac.common.PeerResolver
import io.ktor.application.Application

fun Application.gmmfRouting(
    multiplePeersetProtocols: MultiplePeersetProtocols,
    peerResolver: PeerResolver,
) {
    gmmfNaiveRouting(multiplePeersetProtocols, peerResolver)
    gmmfIndexedRouting(multiplePeersetProtocols, peerResolver)
    gmmfGraphModificationRouting(multiplePeersetProtocols)
}
