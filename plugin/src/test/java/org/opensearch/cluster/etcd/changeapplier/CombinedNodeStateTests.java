/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * State-space tests for {@link CombinedNodeState}: a node that both holds shards ({@code local_shards}) and
 * coordinates indices ({@code remote_shards}). Each test documents the intended cluster-state outcome for a
 * distinct combination of held vs coordinated shard roles, and (except where noted) calls
 * {@link ClusterState#getRoutingNodes()} under assertions so the per-shard {@code RoutingNodes} invariant
 * ("a non-search-only index must have a primary for every shard") is actually exercised for the novel shapes.
 */
public class CombinedNodeStateTests extends OpenSearchTestCase {

    private static final String LOCAL = "local-node-id";
    private static final String INDEX = "idx";

    private final IndicesService indicesService = mock(IndicesService.class);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Force canonicalMapping down its "index not open locally" branch, building the mapping from the map.
        when(indicesService.indexService(any())).thenReturn(null);
    }

    // ---------------------------------------------------------------------------------------------------
    // Single-role pass-through: a combined node with only one kind of key behaves like the dedicated role.
    // ---------------------------------------------------------------------------------------------------

    public void testHeldOnlyIndexHasLocalPrimaryAndIsNotSearchOnly() {
        CombinedNodeState state = combined(
            Map.of(INDEX, components(1)),
            Map.of(INDEX, Set.of(primaryShard(0))),
            List.of(),
            Map.of(), // no remote_shards for this index
            Map.of()
        );

        ClusterState clusterState = state.buildClusterState(ClusterState.EMPTY_STATE, indicesService);

        assertFalse(searchOnly(clusterState));
        ShardRouting primary = clusterState.routingTable().index(INDEX).shard(0).primaryShard();
        assertNotNull(primary);
        assertEquals(LOCAL, primary.currentNodeId());
        assertNoThrowBuildingRoutingNodes(clusterState);
    }

    public void testRemoteOnlyIndexHasRemotePrimaryAndIsNotSearchOnly() {
        // Both referenced nodes must exist as discovery nodes so the routing is consistent.
        CombinedNodeState state = combined(
            Map.of(), // not held
            Map.of(),
            List.of(remote("remote-a", 9401), remote("remote-b", 9402)),
            Map.of(INDEX, List.of(shard(primaryOn("remote-a"), searchReplicaOn("remote-b")))),
            Map.of()
        );

        ClusterState clusterState = state.buildClusterState(ClusterState.EMPTY_STATE, indicesService);

        assertFalse(searchOnly(clusterState));
        assertEquals("remote-a", clusterState.routingTable().index(INDEX).shard(0).primaryShard().currentNodeId());
        assertNoThrowBuildingRoutingNodes(clusterState);
    }

    // ---------------------------------------------------------------------------------------------------
    // The combined cases: an index that is both held and coordinated.
    // ---------------------------------------------------------------------------------------------------

    /** Node is the primary for the shard it also coordinates: real local primary + remote search replica. */
    public void testHeldPrimaryWithCoordinatedSearchReplica() {
        CombinedNodeState state = combined(
            Map.of(INDEX, components(1)),
            Map.of(INDEX, Set.of(primaryShard(0))),
            List.of(remote("remote-b", 9402)),
            Map.of(INDEX, List.of(shard(primaryOn(LOCAL), searchReplicaOn("remote-b")))),
            Map.of()
        );

        ClusterState clusterState = state.buildClusterState(ClusterState.EMPTY_STATE, indicesService);

        IndexShardRoutingTable shard = clusterState.routingTable().index(INDEX).shard(0);
        assertFalse(searchOnly(clusterState));
        assertEquals(LOCAL, shard.primaryShard().currentNodeId());
        assertEquals("the local primary is not duplicated by the remote enumeration", 2, shard.size());
        assertNoThrowBuildingRoutingNodes(clusterState);
    }

    /**
     * THE motivating case: the node holds only a (read-only) search replica of the shard, whose primary lives
     * on another node. The merge must keep the remote primary so writes route there while reads serve locally,
     * and the index must NOT be search-only.
     */
    public void testHeldSearchReplicaWithRemotePrimaryKeepsPrimaryRouting() {
        CombinedNodeState state = combined(
            Map.of(INDEX, components(1)),
            Map.of(INDEX, Set.of(searchReplicaShard(0))),
            List.of(remote("remote-a", 9401)),
            // remote enumerates BOTH the remote primary and this node's own search-replica copy.
            Map.of(INDEX, List.of(shard(primaryOn("remote-a"), searchReplicaOn(LOCAL)))),
            Map.of()
        );

        ClusterState clusterState = state.buildClusterState(ClusterState.EMPTY_STATE, indicesService);

        IndexShardRoutingTable shard = clusterState.routingTable().index(INDEX).shard(0);
        assertFalse("a remote primary exists, so the index is writable, not search-only", searchOnly(clusterState));
        assertNotNull("write routing target", shard.primaryShard());
        assertEquals("remote-a", shard.primaryShard().currentNodeId());
        // The local search replica is present exactly once (its synthetic remote duplicate was skipped).
        List<ShardRouting> localCopies = shard.shards().stream().filter(s -> LOCAL.equals(s.currentNodeId())).toList();
        assertEquals(1, localCopies.size());
        assertFalse("local copy is a read-only search replica", localCopies.get(0).primary());
        assertEquals("primary + local search replica", 2, shard.size());
        // The novel shape: held (initializing) search replica + remote STARTED primary in one index.
        assertNoThrowBuildingRoutingNodes(clusterState);
    }

    /** No primary anywhere (held search replica, coordinated search replicas only) -> the index is search-only. */
    public void testNoPrimaryAnywhereIsSearchOnly() {
        CombinedNodeState state = combined(
            Map.of(INDEX, components(1)),
            Map.of(INDEX, Set.of(searchReplicaShard(0))),
            List.of(remote("remote-b", 9402)),
            Map.of(INDEX, List.of(shard(searchReplicaOn(LOCAL), searchReplicaOn("remote-b")))),
            Map.of()
        );

        ClusterState clusterState = state.buildClusterState(ClusterState.EMPTY_STATE, indicesService);

        assertTrue(searchOnly(clusterState));
        assertNull(clusterState.routingTable().index(INDEX).shard(0).primaryShard());
        // search-only exempts the primary-less shard from the RoutingNodes assertion.
        assertNoThrowBuildingRoutingNodes(clusterState);
    }

    /** A multi-shard index: shard 0 held as primary, shard 1 coordinated with its primary on a remote node. */
    public void testMultiShardHeldPrimaryPlusCoordinatedPrimary() {
        CombinedNodeState state = combined(
            Map.of(INDEX, components(2)),
            Map.of(INDEX, Set.of(primaryShard(0))),
            List.of(remote("remote-a", 9401)),
            Map.of(
                INDEX,
                List.of(
                    shard(primaryOn(LOCAL)),       // shard 0 — held locally
                    shard(primaryOn("remote-a"))   // shard 1 — coordinated only
                )
            ),
            Map.of()
        );

        ClusterState clusterState = state.buildClusterState(ClusterState.EMPTY_STATE, indicesService);

        assertFalse(searchOnly(clusterState));
        assertEquals(LOCAL, clusterState.routingTable().index(INDEX).shard(0).primaryShard().currentNodeId());
        assertEquals("remote-a", clusterState.routingTable().index(INDEX).shard(1).primaryShard().currentNodeId());
        // The coordinated-only shard still gets a primary term for metadata consistency.
        assertEquals(1L, clusterState.metadata().index(INDEX).primaryTerm(1));
        assertNoThrowBuildingRoutingNodes(clusterState);
    }

    /** remote_clusters are surfaced as persistent settings (cross-cluster search wiring). */
    public void testRemoteClustersBecomePersistentSettings() {
        CombinedNodeState state = combined(
            Map.of(INDEX, components(1)),
            Map.of(INDEX, Set.of(primaryShard(0))),
            List.of(),
            Map.of(),
            Map.of("cluster.remote.other.seeds", Map.of("cluster.remote.other.seeds", "10.0.0.1:9300"))
        );

        ClusterState clusterState = state.buildClusterState(ClusterState.EMPTY_STATE, indicesService);

        assertEquals("10.0.0.1:9300", clusterState.metadata().persistentSettings().get("cluster.remote.other.seeds"));
    }

    // ---------------------------------------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------------------------------------

    private CombinedNodeState combined(
        Map<String, IndexMetadataComponents> indices,
        Map<String, Set<DataNodeShard>> assignedShards,
        Collection<RemoteNode> remoteNodes,
        Map<String, List<List<NodeShardAssignment>>> remoteShardAssignments,
        Map<String, Map<String, Object>> remoteClusters
    ) {
        return new CombinedNodeState(localNode(), indices, assignedShards, remoteNodes, remoteShardAssignments, Map.of(), remoteClusters);
    }

    private static DiscoveryNode localNode() {
        return new RemoteNode("local", LOCAL, LOCAL + "-eph", "127.0.0.1", 9300).toDiscoveryNode();
    }

    private static RemoteNode remote(String id, int port) {
        return new RemoteNode(id, id, id + "-eph", "127.0.0.1", port);
    }

    private static IndexMetadataComponents components(int numberOfShards) {
        return new IndexMetadataComponents(
            Map.of("index", Map.of("number_of_shards", Integer.toString(numberOfShards), "number_of_replicas", "0")),
            Map.of("properties", Map.of("field1", Map.of("type", "text"))),
            null
        );
    }

    private static DataNodeShard primaryShard(int shardNum) {
        return new DataNodeShard.SegRepPrimary(INDEX, shardNum, null);
    }

    private static DataNodeShard searchReplicaShard(int shardNum) {
        return new DataNodeShard.SegRepSearchReplica(INDEX, shardNum, null);
    }

    private static NodeShardAssignment primaryOn(String nodeId) {
        return new NodeShardAssignment(nodeId, ShardRole.PRIMARY);
    }

    private static NodeShardAssignment searchReplicaOn(String nodeId) {
        return new NodeShardAssignment(nodeId, ShardRole.SEARCH_REPLICA);
    }

    @SafeVarargs
    private static List<NodeShardAssignment> shard(NodeShardAssignment... assignments) {
        return List.of(assignments);
    }

    private static boolean searchOnly(ClusterState clusterState) {
        return clusterState.metadata()
            .index(INDEX)
            .getSettings()
            .getAsBoolean(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), false);
    }

    private static void assertNoThrowBuildingRoutingNodes(ClusterState clusterState) {
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        assertNotNull(routingNodes);
    }
}
