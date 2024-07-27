package com.github.davenury.ucac.gmmf.routing

import com.github.davenury.ucac.common.MultiplePeersetProtocols
import io.ktor.application.Application

fun Application.gmmfRouting(multiplePeersetProtocols: MultiplePeersetProtocols) {
    gmmfNaiveRouting(multiplePeersetProtocols)
    gmmfGraphModificationRouting(multiplePeersetProtocols)
}
