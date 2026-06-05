/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Cluster state for a node carrying BOTH the data role ({@code local_shards}) and the coordinator role
 * ({@code remote_shards}): it hosts some shards itself and coordinates the rest, so a data node can
 * self-coordinate and dedicated coordinator pods can be dropped.
 *
 * <p>The merge is per-(shard, node): {@code remote_shards} (the cluster-wide routing view) drives the shard
 * list and supplies the routing for every node's copy as synthetic STARTED, EXCEPT this node's own copy of a
 * shard it also holds locally — there the real held routing (real recovery source, allocation-id
 * preservation, previous-shard reuse) is used instead, and the synthetic remote entry for this node is
 * skipped. Keeping the remote primary entry is what lets a node holding only a (read-only) search replica of
 * a shard still route writes to the primary elsewhere while serving reads locally.
 *
 * <p>Held-index metadata (mappings, settings, ingestion status, versioning) matches {@link DataNodeState};
 * remote-only indices get the minimal metadata of {@link CoordinatorNodeState}. The search-only block is
 * computed across the merged routing via {@link IndexStateAssembler#finalizeSearchOnly}.
 */
public class CombinedNodeState extends NodeState {

    private final Map<String, IndexMetadataComponents> indices;
    private final Map<String, Set<DataNodeShard>> assignedShards;
    private final Collection<RemoteNode> remoteNodes;
    private final Map<String, List<List<NodeShardAssignment>>> remoteShardAssignments;
    private final Map<String, Object> aliases;
    private final Map<String, Map<String, Object>> remoteClusters;

    public CombinedNodeState(
        DiscoveryNode localNode,
        Map<String, IndexMetadataComponents> indices,
        Map<String, Set<DataNodeShard>> assignedShards,
        Collection<RemoteNode> remoteNodes,
        Map<String, List<List<NodeShardAssignment>>> remoteShardAssignments,
        Map<String, Object> aliases,
        Map<String, Map<String, Object>> remoteClusters
    ) {
        super(localNode);
        // Held index metadata and held shard assignment cover the same indices.
        assert indices.keySet().equals(assignedShards.keySet());
        this.indices = indices;
        this.assignedShards = assignedShards;
        this.remoteNodes = remoteNodes;
        this.remoteShardAssignments = remoteShardAssignments;
        this.aliases = aliases;
        this.remoteClusters = remoteClusters;
    }

    @Override
    public ClusterState buildClusterState(ClusterState previousState, IndicesService indicesService) {
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(ClusterState.EMPTY_STATE);
        clusterStateBuilder.version(previousState.version() + 1);

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode);
        for (RemoteNode remoteNode : remoteNodes) {
            nodesBuilder.add(remoteNode.toDiscoveryNode());
        }
        // Peer nodes referenced by held docrep shards are added by contributeHeldShard.

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();

        Set<String> allIndices = new LinkedHashSet<>();
        allIndices.addAll(indices.keySet());
        allIndices.addAll(remoteShardAssignments.keySet());

        for (String indexName : allIndices) {
            Index index = new Index(indexName, indexName);
            boolean held = indices.containsKey(indexName);
            List<List<NodeShardAssignment>> remoteRouting = remoteShardAssignments.get(indexName);

            IndexMetadata oldIndexMetadata = previousState.metadata().index(index);
            IndexRoutingTable previousIndexRoutingTable = previousState.routingTable().index(index);
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
            IndexStateAssembler.IndexShardSummary shardSummary = new IndexStateAssembler.IndexShardSummary();

            IndexMetadata.Builder indexMetadataBuilder;
            Settings.Builder settingsBuilder;
            if (held) {
                IndexMetadataComponents components = indices.get(indexName);
                indexMetadataBuilder = IndexMetadata.builder(indexName);
                indexMetadataBuilder.putMapping(
                    new MappingMetadata(ClusterStateUtils.canonicalMapping(index, components.mappings(), indicesService))
                );
                settingsBuilder = ClusterStateUtils.initializeSettingsBuilder(indexName, components.settings());
                indexMetadataBuilder.settings(settingsBuilder);
                ClusterStateUtils.setIngestionStatus(indexMetadataBuilder, components.additionalMetadata(), indexName);
                if (oldIndexMetadata != null) {
                    indexMetadataBuilder.version(oldIndexMetadata.getVersion())
                        .settingsVersion(oldIndexMetadata.getSettingsVersion())
                        .mappingVersion(oldIndexMetadata.getMappingVersion());
                }
            } else {
                int numberOfShards = remoteRouting == null ? 0 : remoteRouting.size();
                settingsBuilder = Settings.builder()
                    .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
                indexMetadataBuilder = IndexMetadata.builder(indexName).settings(settingsBuilder);
            }

            if (remoteRouting != null) {
                mergeRoutedIndex(
                    index,
                    held,
                    remoteRouting,
                    previousIndexRoutingTable,
                    indexRoutingTableBuilder,
                    indexMetadataBuilder,
                    nodesBuilder,
                    shardSummary
                );
            } else {
                // Held-only index (not coordinated): identical to the data-node path.
                for (DataNodeShard dataNodeShard : assignedShards.get(indexName)) {
                    IndexStateAssembler.addHeldShard(
                        index,
                        dataNodeShard,
                        localNode,
                        previousIndexRoutingTable,
                        indexRoutingTableBuilder,
                        indexMetadataBuilder,
                        nodesBuilder,
                        shardSummary
                    );
                }
            }

            IndexStateAssembler.finalizeSearchOnly(shardSummary, settingsBuilder);
            indexMetadataBuilder.settings(settingsBuilder);
            ClusterStateUtils.addAliasesToIndexMetadata(indexMetadataBuilder, indexName, aliases);

            IndexMetadata newIndexMetadata = held
                ? ClusterStateUtils.applyVersioning(indexName, indexMetadataBuilder, oldIndexMetadata)
                : indexMetadataBuilder.build();

            routingTableBuilder.add(indexRoutingTableBuilder);
            metadataBuilder.put(newIndexMetadata, false);
        }

        if (remoteClusters != null && remoteClusters.isEmpty() == false) {
            Settings.Builder persistentSettingsBuilder = Settings.builder();
            for (Map<String, Object> remoteSettingMap : remoteClusters.values()) {
                persistentSettingsBuilder.loadFromMap(remoteSettingMap);
            }
            metadataBuilder.persistentSettings(persistentSettingsBuilder.build());
        }

        clusterStateBuilder.nodes(nodesBuilder);
        clusterStateBuilder.routingTable(routingTableBuilder.build());
        clusterStateBuilder.metadata(metadataBuilder);
        return clusterStateBuilder.build();
    }

    /**
     * Builds the routing for an index that this node coordinates, merging — per shard — the real routing of
     * any shard this node also holds locally with the synthetic remote routing of the other nodes' copies.
     */
    private void mergeRoutedIndex(
        Index index,
        boolean held,
        List<List<NodeShardAssignment>> remoteRouting,
        IndexRoutingTable previousIndexRoutingTable,
        IndexRoutingTable.Builder indexRoutingTableBuilder,
        IndexMetadata.Builder indexMetadataBuilder,
        DiscoveryNodes.Builder nodesBuilder,
        IndexStateAssembler.IndexShardSummary shardSummary
    ) {
        Map<Integer, DataNodeShard> heldByShard = held ? heldShardsByNum(index.getName()) : Map.of();
        // remote_shards is the complete shard list; also cover any held shard beyond it (controller contract gap).
        int numShards = Math.max(remoteRouting.size(), maxShardNum(heldByShard) + 1);

        for (int shardNum = 0; shardNum < numShards; shardNum++) {
            ShardId shardId = new ShardId(index, shardNum);
            IndexShardRoutingTable.Builder shardTableBuilder = new IndexShardRoutingTable.Builder(shardId);
            DataNodeShard heldShard = heldByShard.get(shardNum);

            String skipNodeId = null;
            if (heldShard != null) {
                IndexStateAssembler.contributeHeldShard(
                    index,
                    heldShard,
                    localNode,
                    previousIndexRoutingTable,
                    shardTableBuilder,
                    indexMetadataBuilder,
                    nodesBuilder
                );
                // The held contributor provides this node's real routing; skip its synthetic remote duplicate.
                skipNodeId = localNode.getId();
            } else if (held) {
                // A coordinated-only shard of a held index still needs a primary term for metadata consistency.
                indexMetadataBuilder.primaryTerm(shardNum, 1);
            }

            if (shardNum < remoteRouting.size()) {
                IndexStateAssembler.contributeRemoteShard(index, shardNum, remoteRouting.get(shardNum), skipNodeId, shardTableBuilder);
            }

            IndexShardRoutingTable shardTable = shardTableBuilder.build();
            shardSummary.recordShard(shardTable.primaryShard() != null);
            indexRoutingTableBuilder.addIndexShard(shardTable);
        }
    }

    private Map<Integer, DataNodeShard> heldShardsByNum(String indexName) {
        Set<DataNodeShard> shards = assignedShards.get(indexName);
        if (shards == null) {
            return Map.of();
        }
        Map<Integer, DataNodeShard> byNum = new HashMap<>();
        for (DataNodeShard shard : shards) {
            byNum.put(shard.getShardNum(), shard);
        }
        return byNum;
    }

    private static int maxShardNum(Map<Integer, DataNodeShard> heldByShard) {
        int max = -1;
        for (int shardNum : heldByShard.keySet()) {
            max = Math.max(max, shardNum);
        }
        return max;
    }
}
