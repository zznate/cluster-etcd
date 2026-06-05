/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IngestionStatus;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Index-metadata building utilities shared by the per-node {@link NodeState} implementations: index
 * settings initialization, mapping canonicalization, ingestion status, and metadata version bumping.
 */
class ClusterStateUtils {
    private static final Logger logger = LogManager.getLogger(ClusterStateUtils.class);
    private static final String PAUSE_PULL_INGESTION_KEY = "pause_pull_ingestion";

    static Settings.Builder initializeSettingsBuilder(String indexName, Map<String, Object> settingsMap) {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.loadFromMap(settingsMap);
        settingsBuilder.put(IndexMetadata.SETTING_INDEX_UUID, indexName);
        settingsBuilder.put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
        settingsBuilder.put(IndexMetadata.SETTING_CREATION_DATE, generateDeterministicCreationDate(indexName));
        return settingsBuilder;
    }

    /**
     * Generates a deterministic creation date based on the index name.
     * This ensures all nodes generate the same creation date for the same index.
     */
    private static long generateDeterministicCreationDate(String indexName) {
        // Generate a deterministic timestamp based on index name
        // This ensures all nodes generate the same creation date for the same index
        // Use a fixed epoch time (e.g., 2024-01-01) plus a hash of the index name
        long baseEpoch = 1704067200000L; // 2024-01-01 00:00:00 UTC

        // Generate a deterministic offset based on index name hash
        int hashOffset = (indexName.hashCode() & Integer.MAX_VALUE) % (24 * 60 * 60 * 1000);

        return baseEpoch + hashOffset;
    }

    /**
     * Produces the canonical compressed mapping for an index, merging the goal-state mapping into the
     * locally-known mapping when the index is already open on this node.
     */
    static CompressedXContent canonicalMapping(Index index, Map<String, Object> newMapping, IndicesService indicesService) {
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

    /**
     * Sets the ingestion status on the index metadata builder based on the pause_pull_ingestion value
     * from additional metadata.
     */
    static void setIngestionStatus(IndexMetadata.Builder indexMetadataBuilder, Map<String, Object> additionalMetadata, String indexName) {
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

    /**
     * Adds, to the index metadata builder, every alias whose target set includes this index.
     */
    static void addAliasesToIndexMetadata(IndexMetadata.Builder indexMetadataBuilder, String indexName, Map<String, Object> aliases) {
        if (aliases == null || aliases.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Object> aliasEntry : aliases.entrySet()) {
            String aliasName = aliasEntry.getKey();
            Object aliasValue = aliasEntry.getValue();
            if (isAliasForIndex(aliasValue, indexName)) {
                indexMetadataBuilder.putAlias(AliasMetadata.builder(aliasName).build());
            }
        }
    }

    /**
     * Checks whether the given alias value (a single index name or a list of names) targets this index.
     */
    private static boolean isAliasForIndex(Object aliasValue, String indexName) {
        switch (aliasValue) {
            case String s -> {
                return indexName.equals(s);
            }
            case List<?> list -> {
                return list.contains(indexName);
            }
            default -> {
                return false;
            }
        }
    }

    /**
     * Bumps the index metadata/settings/mapping versions relative to the previous cluster state when the
     * newly built metadata differs, mirroring how a cluster manager advances these versions on change.
     */
    static IndexMetadata applyVersioning(String indexName, IndexMetadata.Builder indexMetadataBuilder, IndexMetadata oldIndexMetadata) {
        IndexMetadata newIndexMetadata = indexMetadataBuilder.build();
        if (oldIndexMetadata != null && oldIndexMetadata.equals(newIndexMetadata) == false) {
            logger.info("Index metadata for index {} changed", indexName);
            indexMetadataBuilder.version(oldIndexMetadata.getVersion() + 1);
            if (oldIndexMetadata.getSettings().equals(newIndexMetadata.getSettings()) == false) {
                indexMetadataBuilder.settingsVersion(oldIndexMetadata.getSettingsVersion() + 1);
            }
            if (Objects.equals(oldIndexMetadata.mapping(), newIndexMetadata.mapping()) == false) {
                indexMetadataBuilder.mappingVersion(oldIndexMetadata.getMappingVersion() + 1);
            }
            newIndexMetadata = indexMetadataBuilder.build();
        }
        return newIndexMetadata;
    }
}
