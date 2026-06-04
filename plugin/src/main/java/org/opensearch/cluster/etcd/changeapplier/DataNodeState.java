/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionStatus;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class DataNodeState extends NodeState {
    private static final Logger logger = LogManager.getLogger(DataNodeState.class);
    private static final String PAUSE_PULL_INGESTION_KEY = "pause_pull_ingestion";

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

    private static CompressedXContent canonicalMapping(Index index, Map<String, Object> newMapping, IndicesService indicesService) {
        try {
            IndexService indexService = indicesService.indexService(index);
            if (indexService == null) {
                Map<String, Object> topLevelMapping = new HashMap<>();
                topLevelMapping.put(MapperService.SINGLE_MAPPING_NAME, newMapping);
                XContentBuilder mappingBuilder = XContentFactory.jsonBuilder().map(topLevelMapping);
                return new CompressedXContent(BytesReference.bytes(mappingBuilder));
            }
            IndexMetadata indexMetadata = indexService.getMetadata();
            try (MapperService mapperService = indicesService.createIndexMapperService(indexMetadata)) {
                DocumentMapper existingDocumentMapper = mapperService.documentMapperParser()
                    .parse(MapperService.SINGLE_MAPPING_NAME, indexMetadata.mapping().source());
                DocumentMapper newDocumentMapper = mapperService.documentMapperParser()
                    .parse(MapperService.SINGLE_MAPPING_NAME, newMapping);
                DocumentMapper mergedDocumentMapper = existingDocumentMapper.merge(
                    newDocumentMapper.mapping(),
                    MapperService.MergeReason.MAPPING_UPDATE
                );
                return mergedDocumentMapper.mappingSource();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse mapping for index " + index.getName(), e);
        }
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
            indexMetadataBuilder.putMapping(new MappingMetadata(canonicalMapping(index, entry.getValue().mappings(), indicesService)));
            Settings.Builder settingsBuilder = ClusterStateUtils.initializeSettingsBuilder(entry.getKey(), entry.getValue().settings());
            indexMetadataBuilder.settings(settingsBuilder);

            // Set ingestion status based on pause_pull_ingestion from additionalMetadata
            setIngestionStatus(indexMetadataBuilder, entry.getValue().additionalMetadata(), entry.getKey());

            if (oldIndexMetadata != null) {
                indexMetadataBuilder.version(oldIndexMetadata.getVersion())
                    .settingsVersion(oldIndexMetadata.getSettingsVersion())
                    .mappingVersion(oldIndexMetadata.getMappingVersion());
            }
            for (DataNodeShard dataNodeShard : assignedShards.get(index.getName())) {
                IndexStateAssembler.contributeHeldShard(
                    index,
                    dataNodeShard,
                    localNode,
                    previousIndexRoutingTable,
                    indexMetadataBuilder,
                    indexRoutingTableBuilder,
                    nodesBuilder,
                    settingsBuilder
                );
                // Set settings again, in case the held-shard contributor modified it above
                indexMetadataBuilder.settings(settingsBuilder);
            }
            IndexMetadata newIndexMetadata = indexMetadataBuilder.build();
            if (oldIndexMetadata != null && oldIndexMetadata.equals(newIndexMetadata) == false) {
                logger.info("Index metadata for index {} changed", index.getName());
                indexMetadataBuilder.version(oldIndexMetadata.getVersion() + 1);
                if (oldIndexMetadata.getSettings().equals(newIndexMetadata.getSettings()) == false) {
                    indexMetadataBuilder.settingsVersion(oldIndexMetadata.getSettingsVersion() + 1);
                }
                if (Objects.equals(oldIndexMetadata.mapping(), newIndexMetadata.mapping()) == false) {
                    indexMetadataBuilder.mappingVersion(oldIndexMetadata.getMappingVersion() + 1);
                }
                newIndexMetadata = indexMetadataBuilder.build();
            }

            routingTableBuilder.add(indexRoutingTableBuilder);
            metadataBuilder.put(newIndexMetadata, false);
        }
        clusterStateBuilder.routingTable(routingTableBuilder.build());
        clusterStateBuilder.metadata(metadataBuilder);

        return clusterStateBuilder.build();
    }

    /**
     * Sets the ingestion status on the index metadata builder based on the pause_pull_ingestion value
     * from additional metadata.
     */
    private static void setIngestionStatus(
        IndexMetadata.Builder indexMetadataBuilder,
        Map<String, Object> additionalMetadata,
        String indexName
    ) {
        if (additionalMetadata == null) {
            return;
        }

        Object pausePullIngestionValue = additionalMetadata.get(PAUSE_PULL_INGESTION_KEY);
        if (pausePullIngestionValue != null) {
            boolean pausePullIngestion = pausePullIngestionValue instanceof Boolean
                ? (Boolean) pausePullIngestionValue
                : Boolean.parseBoolean(String.valueOf(pausePullIngestionValue));
            indexMetadataBuilder.ingestionStatus(new IngestionStatus(pausePullIngestion));
            logger.debug("Set ingestion status for index {}: pause_pull_ingestion={}", indexName, pausePullIngestion);
        }
    }
}
