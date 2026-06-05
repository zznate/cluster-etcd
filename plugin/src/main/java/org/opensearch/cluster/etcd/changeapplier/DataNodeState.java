/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.indices.IndicesService;

import java.util.Map;
import java.util.Set;

public class DataNodeState extends NodeState {

    private final Map<String, IndexMetadataComponents> indices;
    private final Map<String, Set<DataNodeShard>> assignedShards;

    public DataNodeState(
        DiscoveryNode localNode,
        Map<String, IndexMetadataComponents> indices,
        Map<String, Set<DataNodeShard>> assignedShards
    ) {
        super(localNode);
        // The index metadata and shard assignment should be identical
        assert indices.keySet().equals(assignedShards.keySet());
        this.indices = indices;
        this.assignedShards = assignedShards;
    }

    @Override
    public ClusterState buildClusterState(ClusterState previousState, IndicesService indicesService) {
        ClusterState.Builder clusterStateBuilder = ClusterState.builder(ClusterState.EMPTY_STATE);
        clusterStateBuilder.version(previousState.version() + 1);
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode);

        clusterStateBuilder.nodes(nodesBuilder);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();
        for (Map.Entry<String, IndexMetadataComponents> entry : indices.entrySet()) {
            Index index = new Index(entry.getKey(), entry.getKey());
            IndexMetadata oldIndexMetadata = previousState.metadata().index(index);

            IndexRoutingTable previousIndexRoutingTable = previousState.routingTable().index(index);
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(entry.getKey());
            indexMetadataBuilder.putMapping(
                new MappingMetadata(ClusterStateUtils.canonicalMapping(index, entry.getValue().mappings(), indicesService))
            );
            Settings.Builder settingsBuilder = ClusterStateUtils.initializeSettingsBuilder(entry.getKey(), entry.getValue().settings());
            indexMetadataBuilder.settings(settingsBuilder);

            // Set ingestion status based on pause_pull_ingestion from additionalMetadata
            ClusterStateUtils.setIngestionStatus(indexMetadataBuilder, entry.getValue().additionalMetadata(), entry.getKey());

            if (oldIndexMetadata != null) {
                indexMetadataBuilder.version(oldIndexMetadata.getVersion())
                    .settingsVersion(oldIndexMetadata.getSettingsVersion())
                    .mappingVersion(oldIndexMetadata.getMappingVersion());
            }
            IndexStateAssembler.IndexShardSummary shardSummary = new IndexStateAssembler.IndexShardSummary();
            for (DataNodeShard dataNodeShard : assignedShards.get(index.getName())) {
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
            IndexStateAssembler.finalizeSearchOnly(shardSummary, settingsBuilder);
            indexMetadataBuilder.settings(settingsBuilder);
            IndexMetadata newIndexMetadata = ClusterStateUtils.applyVersioning(index.getName(), indexMetadataBuilder, oldIndexMetadata);

            routingTableBuilder.add(indexRoutingTableBuilder);
            metadataBuilder.put(newIndexMetadata, false);
        }
        clusterStateBuilder.routingTable(routingTableBuilder.build());
        clusterStateBuilder.metadata(metadataBuilder);

        return clusterStateBuilder.build();
    }
}
