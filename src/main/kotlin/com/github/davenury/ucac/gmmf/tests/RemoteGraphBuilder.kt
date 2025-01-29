package com.github.davenury.ucac.gmmf.tests

import com.github.davenury.common.PeerId
import com.github.davenury.common.PeersetId
import com.github.davenury.ucac.common.PeerResolver
import com.github.davenury.ucac.gmmf.client.GmmfClient
import com.github.kjarosh.agh.pp.graph.model.Edge
import com.github.kjarosh.agh.pp.graph.model.Graph
import com.github.kjarosh.agh.pp.graph.model.Vertex
import com.github.kjarosh.agh.pp.graph.model.ZoneId
import com.github.kjarosh.agh.pp.rest.dto.BulkVertexCreationRequestDto
import com.github.kjarosh.agh.pp.rest.dto.EdgeCreationRequestDto
import com.github.kjarosh.agh.pp.rest.dto.VertexCreationRequestDto
import com.google.common.collect.Lists
import kotlinx.coroutines.runBlocking
import org.apache.commons.lang3.tuple.Pair
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.EnumSet
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import java.util.stream.Collectors
import java.util.stream.Collectors.toSet

/**
 * @author Kamil Jarosz
 */
class RemoteGraphBuilder(private val graph: Graph, private val peerResolver: PeerResolver) {
    private val verticesBuilt = AtomicInteger(0)
    private val edgesBuilt = AtomicInteger(0)

    private val clients: MutableMap<PeerId, GmmfClient> = mutableMapOf()

    private fun getClient(zoneId: ZoneId): GmmfClient {
        return getClient(PeersetId(zoneId.id))
    }

    private fun getClient(peersetId: PeersetId): GmmfClient {
        val peer = peerResolver.getPeerFromPeerset(peersetId)
        return clients.computeIfAbsent(peer.peerId) {
            GmmfClient(peerResolver, peer)
        }
    }

    private fun getClient(peerId: PeerId): GmmfClient {
        return clients.computeIfAbsent(peerId) {
            GmmfClient(peerResolver, peerResolver.resolve(peerId))
        }
    }

    fun build(vararg options: BulkOption?) {
        val optionsSet = if (options.isEmpty()) EnumSet.noneOf(BulkOption::class.java) else EnumSet.copyOf(listOf(*options))
        logger.info("Building graph")
        val start = Instant.now()

        val allPeers =
            graph.allZones()
                .stream()
                .filter { z -> z != null }
                .map { zone: ZoneId -> PeersetId(zone.toString()) }
                .flatMap { peersetId -> peerResolver.getPeersFromPeerset(peersetId).stream() }
                .map { address -> address.peerId }
                .collect(toSet())

        logger.info("Checking all peers if they are healthy: {}", allPeers)
        while (!healthy(allPeers)) {
            sleep()
        }

        val supervisor =
            Supervisor(
                { verticesBuilt.get().toDouble() / graph.allVertices().size },
                { edgesBuilt.get().toDouble() / graph.allEdges().size },
            )
        supervisor.start()
        try {
            if (!optionsSet.contains(BulkOption.NO_BULK_VERTICES)) {
                logger.info("Building vertices in bulk")
                buildVerticesBulk()
            } else {
                logger.info("Building vertices")
                buildVertices()
            }

            if (!optionsSet.contains(BulkOption.NO_BULK_EDGES)) {
                logger.info("Building edges in bulk")
                buildEdgesBulk()
            } else {
                logger.info("Building edges")
                buildEdges()
            }

            logger.info("Waiting for index to be built: {}", allPeers)
            // TODO client.waitForIndex(allZones, Duration.ofHours(2))
        } catch (e: TimeoutException) {
            throw RuntimeException(e)
        } finally {
            supervisor.interrupt()
        }

        try {
            supervisor.join()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        }

        logger.info("Graph built in {}", Duration.between(start, Instant.now()))
    }

    private fun sleep() {
        try {
            Thread.sleep(1000)
        } catch (e: InterruptedException) {
            throw RuntimeException("Interrupted while building graph")
        }
    }

    private fun healthy(allPeers: Collection<PeerId>): Boolean {
        val notHealthy = allPeers.filter { peerId -> runBlocking { !getClient(peerId).healthcheck(peerId) } }
        if (notHealthy.isEmpty()) {
            logger.info("All peers healthy")
            return true
        } else {
            logger.info("Peers not healthy: {}", notHealthy)
            return false
        }
    }

    private fun buildVerticesBulk() {
        val groupedByOwner =
            graph.allVertices()
                .stream()
                .collect(
                    Collectors.groupingBy { v: Vertex -> v.id().owner() },
                )

        for (owner in groupedByOwner.keys) {
            val client = getClient(owner)
            logger.debug("Sending batches of vertices to {}", owner)
            val vertices = groupedByOwner[owner]!!
            for (bulk in Lists.partition<Vertex>(vertices, BULK_SIZE)) {
                val requests =
                    bulk.stream()
                        .map { v: Vertex ->
                            VertexCreationRequestDto(
                                v.id().name(),
                                v.type(),
                            )
                        }
                        .collect(Collectors.toList())
                logger.debug("Sending a batch of {} vertices to {}", requests.size, owner)
                runBlocking {
                    client.addVertices(PeersetId(owner.id), BulkVertexCreationRequestDto(requests))
                }
                verticesBuilt.addAndGet(requests.size)
            }
            logger.debug("Finished sending batches of vertices to {}", owner)
        }
    }

    private fun buildVertices() {
        graph.allVertices()
            .stream()
            .parallel()
            .forEach { v: Vertex ->
                runBlocking {
                    getClient(v.id().owner()).addVertex(v.id(), v.type())
                    verticesBuilt.incrementAndGet()
                }
            }
    }

    private fun buildEdgesBulk() {
        val grouped =
            graph.allEdges()
                .stream()
                .collect(
                    Collectors.groupingBy { e: Edge ->
                        Pair.of(
                            e.src().owner(),
                            e.dst().owner(),
                        )
                    },
                )

        for (pair in grouped.keys) {
            logger.debug(
                "Sending batches of edges between {} and {}",
                pair.left,
                pair.right,
            )
            val edges = grouped[pair]!!
            for (bulk in Lists.partition<Edge>(edges, BULK_SIZE)) {
                val requests =
                    bulk.stream()
                        .map { e: Edge? -> EdgeCreationRequestDto.fromEdge(e, null) }
                        .collect(Collectors.toList())
                logger.debug("Sending a batch of {} edges between {} and {}", requests.size, pair.left, pair.right)
                /* TODO client.addEdges(
                    pair.left,
                    BulkEdgeCreationRequestDto.builder()
                        .sourceZone(pair.left)
                        .destinationZone(pair.right)
                        .successive(false)
                        .edges(requests)
                        .build(),
                ) */
                edgesBuilt.addAndGet(requests.size)
            }
            logger.debug(
                "Finished sending batches of edges between {} and {}",
                pair.left,
                pair.right,
            )
        }
    }

    private fun buildEdges() {
        graph.allEdges()
            .stream()
            .sorted(
                Comparator.comparing { obj: Edge -> obj.src() }
                    .thenComparing { obj: Edge -> obj.dst() },
            )
            .parallel()
            .forEach { e: Edge ->
                runBlocking {
                    edgesBuilt.getAndIncrement()
                    getClient(e.id().from.owner()).addEdge(e.id(), e.permissions())
                }
            }
    }

    enum class BulkOption {
        NO_BULK_EDGES,
        NO_BULK_VERTICES,
    }

    companion object {
        private val logger = LoggerFactory.getLogger("remote-graph-builder")
        private const val BULK_SIZE = 200
    }
}
