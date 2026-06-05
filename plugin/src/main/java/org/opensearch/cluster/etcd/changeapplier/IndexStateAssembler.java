/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.cluster.etcd.changeapplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Shard-level contributors shared by the per-node {@link NodeState} implementations.
 * Each contributor feeds an index's {@link IndexRoutingTable.Builder} / {@link IndexMetadata.Builder} /
 * {@link DiscoveryNodes.Builder} for a single shard, so that a node holding more than one kind of
 * shard for an index assembles its routing and metadata exactly once.
 */
final class IndexStateAssembler {
    private static final Logger logger = LogManager.getLogger(IndexStateAssembler.class);

    private IndexStateAssembler() {}

    /**
     * Appends the routing entries this node contributes as the holder of {@code dataNodeShard} to the given
     * per-shard routing table, reproducing the recovery-source, allocation-id-preservation, peer-wiring,
     * in-sync and primary-term handling of a data node. The caller owns the per-shard
     * {@link IndexShardRoutingTable.Builder} so a combined node can also append remote routings for the same
     * shard before building it.
     *
     * @param index                    the index the shard belongs to
     * @param dataNodeShard            the locally-held shard to contribute
     * @param localNode                this node, the holder of the shard
     * @param previousIndexRoutingTable the index's routing table from the previous cluster state (may be null)
     * @param shardTableBuilder        the per-shard routing table being assembled for this shard
     * @param indexMetadataBuilder     accumulates primary terms and in-sync allocation ids
     * @param nodesBuilder             accumulates discovery nodes referenced by peer (primary/replica) routings
     */
    static void contributeHeldShard(
        Index index,
        DataNodeShard dataNodeShard,
        DiscoveryNode localNode,
        IndexRoutingTable previousIndexRoutingTable,
        IndexShardRoutingTable.Builder shardTableBuilder,
        IndexMetadata.Builder indexMetadataBuilder,
        DiscoveryNodes.Builder nodesBuilder
    ) {
        int shardNum = dataNodeShard.getShardNum();
        indexMetadataBuilder.primaryTerm(shardNum, 1);
        ShardRole role = dataNodeShard.getShardRole();
        ShardId shardId = new ShardId(index, shardNum);

        IndexShardRoutingTable previousShardRoutingTable = previousIndexRoutingTable == null
            ? new IndexShardRoutingTable.Builder(shardId).build()
            : previousIndexRoutingTable.shard(shardNum);

        UnassignedInfo unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "created");

        // Handle replica shards with primary allocation
        if (role == ShardRole.REPLICA && dataNodeShard.getPrimaryAllocation().isPresent()) {
            DataNodeShard.ShardAllocation primaryAllocation = dataNodeShard.getPrimaryAllocation().get();
            RemoteNode primaryNode = primaryAllocation.node();

            if (previousShardRoutingTable.primaryShard() != null
                && previousShardRoutingTable.primaryShard().currentNodeId().equals(primaryNode.nodeId())) {
                shardTableBuilder.addShard(previousShardRoutingTable.primaryShard());
            } else {
                ShardRouting primaryShardRouting = ShardRouting.newUnassigned(
                    shardId,
                    true,
                    false,
                    RecoverySource.ExistingStoreRecoverySource.INSTANCE,
                    unassignedInfo
                )
                    .initialize(primaryNode.nodeId(), primaryAllocation.allocationId(), ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE)
                    .moveToStarted();
                // Add the remote primary shard routing for this held replica
                shardTableBuilder.addShard(primaryShardRouting);
            }
            nodesBuilder.add(primaryNode.toDiscoveryNode());
        }

        Set<String> inSyncAllocationIds = new HashSet<>();
        if (role == ShardRole.PRIMARY && dataNodeShard.getReplicaAssignments().isEmpty() == false) {
            // If this is a primary, we need to add the replica shards
            Map<String, DataNodeShard.ShardAllocation> replicaNodesMap = new HashMap<>(
                dataNodeShard.getReplicaAssignments().stream().collect(Collectors.toMap(k -> k.node().nodeId(), Function.identity()))
            );
            for (DataNodeShard.ShardAllocation shardAllocation : replicaNodesMap.values()) {
                RemoteNode replicaNode = shardAllocation.node();
                ShardRouting replicaShardRouting = ShardRouting.newUnassigned(
                    shardId,
                    false,
                    false,
                    RecoverySource.PeerRecoverySource.INSTANCE,
                    unassignedInfo
                ).initialize(replicaNode.nodeId(), shardAllocation.allocationId(), ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                if (shardAllocation.shardState() == DataNodeShard.ShardState.STARTED) {
                    inSyncAllocationIds.add(shardAllocation.allocationId());
                    replicaShardRouting = replicaShardRouting.moveToStarted();
                }
                // Add the replica shard routing to the index routing table
                shardTableBuilder.addShard(replicaShardRouting);
                nodesBuilder.add(replicaNode.toDiscoveryNode());
            }
        }

        Optional<ShardRouting> previouslyStartedShard = previousShardRoutingTable == null
            ? Optional.empty()
            : previousShardRoutingTable.shards()
                .stream()
                .filter(sr -> localNode.getId().equals(sr.currentNodeId()))
                .filter(s -> s.started() || s.initializing())
                .findAny();

        ShardRouting shardRouting;
        if (previouslyStartedShard.isPresent()) {
            shardRouting = previouslyStartedShard.get();
            logger.debug(
                "Reusing existing ShardRouting for shard {}[{}] with allocation ID {}",
                index.getName(),
                shardNum,
                shardRouting.allocationId().getId()
            );
        } else {
            // No previous shard in cluster state - use ETCD-based allocation ID preservation
            RecoverySource recoverySource = determineRecoverySource(dataNodeShard);
            shardRouting = ShardRouting.newUnassigned(
                shardId,
                role == ShardRole.PRIMARY,
                role == ShardRole.SEARCH_REPLICA,
                recoverySource,
                unassignedInfo
            );

            // Determine recovery source for this shard
            String previousAllocationId = dataNodeShard.getAllocationId();
            shardRouting = shardRouting.initialize(localNode.getId(), previousAllocationId, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);

            // ALLOCATION ID TRACKING: Log allocation ID changes for monitoring purposes
            String currentAllocationId = shardRouting.allocationId().getId();

            if (previousAllocationId != null) {
                if (previousAllocationId.equals(currentAllocationId)) {
                    logger.info(
                        "✅ ALLOCATION ID PRESERVED: shard {}[{}] kept allocation ID {}",
                        index.getName(),
                        shardNum,
                        previousAllocationId
                    );
                } else {
                    logger.info(
                        "🔄 ALLOCATION ID CHANGED: shard {}[{}] from {} to {}",
                        index.getName(),
                        shardNum,
                        previousAllocationId,
                        currentAllocationId
                    );
                }
            } else {
                logger.debug(
                    "No previous allocation ID found for shard {}[{}], using new ID: {}",
                    index.getName(),
                    shardNum,
                    currentAllocationId
                );
            }
        }
        shardTableBuilder.addShard(shardRouting);

        inSyncAllocationIds.add(shardRouting.allocationId().getId());
        indexMetadataBuilder.putInSyncAllocationIds(shardNum, inSyncAllocationIds);
    }

    /**
     * Appends synthetic STARTED routings for the remote assignments of one shard to the given per-shard
     * routing table (a coordinator never holds these shards itself). When {@code skipNodeId} is non-null, the
     * assignment on that node is skipped so a combined node uses its real held routing for its own copy
     * instead of this synthetic one, while still picking up the other nodes (notably the primary).
     *
     * @param index             the index the shard belongs to
     * @param shardNum          the shard number being contributed
     * @param shardAssignments  the per-node assignments for this shard
     * @param skipNodeId        a node id whose assignment to skip, or null to include all
     * @param shardTableBuilder the per-shard routing table being assembled for this shard
     */
    static void contributeRemoteShard(
        Index index,
        int shardNum,
        List<NodeShardAssignment> shardAssignments,
        String skipNodeId,
        IndexShardRoutingTable.Builder shardTableBuilder
    ) {
        ShardId shardId = new ShardId(index, shardNum);
        for (NodeShardAssignment shardAssignment : shardAssignments) {
            if (skipNodeId != null && skipNodeId.equals(shardAssignment.nodeId())) {
                continue;
            }
            ShardRole shardRole = shardAssignment.shardRole();
            ShardRouting nodeEntry = ShardRouting.newUnassigned(
                shardId,
                shardRole == ShardRole.PRIMARY,
                shardRole == ShardRole.SEARCH_REPLICA,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "initializing")
            );
            nodeEntry = nodeEntry.initialize(shardAssignment.nodeId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            nodeEntry = nodeEntry.moveToStarted();
            shardTableBuilder.addShard(nodeEntry);
        }
    }

    /**
     * Single-source convenience for a data node: assemble the routing table for one held shard, record its
     * primary presence, and add it to the index routing table.
     */
    static void addHeldShard(
        Index index,
        DataNodeShard dataNodeShard,
        DiscoveryNode localNode,
        IndexRoutingTable previousIndexRoutingTable,
        IndexRoutingTable.Builder indexRoutingTableBuilder,
        IndexMetadata.Builder indexMetadataBuilder,
        DiscoveryNodes.Builder nodesBuilder,
        IndexShardSummary summary
    ) {
        ShardId shardId = new ShardId(index, dataNodeShard.getShardNum());
        IndexShardRoutingTable.Builder shardTableBuilder = new IndexShardRoutingTable.Builder(shardId);
        contributeHeldShard(
            index,
            dataNodeShard,
            localNode,
            previousIndexRoutingTable,
            shardTableBuilder,
            indexMetadataBuilder,
            nodesBuilder
        );
        addAndRecord(shardTableBuilder, indexRoutingTableBuilder, summary);
    }

    /**
     * Single-source convenience for a coordinator: assemble the routing table for one remote shard, record
     * its primary presence, add it to the index routing table, and report whether it has a primary.
     */
    static boolean addRemoteShard(
        Index index,
        int shardNum,
        List<NodeShardAssignment> shardAssignments,
        IndexRoutingTable.Builder indexRoutingTableBuilder,
        IndexShardSummary summary
    ) {
        ShardId shardId = new ShardId(index, shardNum);
        IndexShardRoutingTable.Builder shardTableBuilder = new IndexShardRoutingTable.Builder(shardId);
        contributeRemoteShard(index, shardNum, shardAssignments, null, shardTableBuilder);
        return addAndRecord(shardTableBuilder, indexRoutingTableBuilder, summary);
    }

    /**
     * Builds a per-shard routing table, records whether it has a primary in {@code summary}, adds it to the
     * index routing table, and returns whether it has a primary.
     */
    private static boolean addAndRecord(
        IndexShardRoutingTable.Builder shardTableBuilder,
        IndexRoutingTable.Builder indexRoutingTableBuilder,
        IndexShardSummary summary
    ) {
        IndexShardRoutingTable shardTable = shardTableBuilder.build();
        boolean shardHasPrimary = shardTable.primaryShard() != null;
        summary.recordShard(shardHasPrimary);
        indexRoutingTableBuilder.addIndexShard(shardTable);
        return shardHasPrimary;
    }

    /**
     * Determines the appropriate recovery source for a held shard.
     * This prevents data loss on node restarts by using existing data when available.
     *
     * @param dataNodeShard the DataNodeShard containing all necessary information
     * @return the recovery source to use for this shard
     */
    private static RecoverySource determineRecoverySource(DataNodeShard dataNodeShard) {
        String indexName = dataNodeShard.getIndexName();
        int shardNum = dataNodeShard.getShardNum();
        ShardRole role = dataNodeShard.getShardRole();

        logger.debug("Determining recovery source for shard {}[{}] with role {}", indexName, shardNum, role);

        // Regular replica shards will recover from primary
        if (role == ShardRole.REPLICA) {
            logger.info("Shard {}[{}] with role {} is replica, using PeerRecoverySource", indexName, shardNum, role);
            return RecoverySource.PeerRecoverySource.INSTANCE;
        }

        // For PRIMARY (and search replica fallthrough) shards: if previously allocated then existing, else empty
        String previousAllocationId = dataNodeShard.getAllocationId();
        if (previousAllocationId != null) {
            logger.info(
                "Shard {}[{}] with role {} was previously allocated (ID: {}), using ExistingStoreRecoverySource",
                indexName,
                shardNum,
                role,
                previousAllocationId
            );
            return RecoverySource.ExistingStoreRecoverySource.INSTANCE;
        } else {
            logger.info(
                "Shard {}[{}] with role {} was not previously allocated, using EmptyStoreRecoverySource",
                indexName,
                shardNum,
                role
            );
            return RecoverySource.EmptyStoreRecoverySource.INSTANCE;
        }
    }

    /**
     * Applies the index-level search-only block from the shards contributed for an index. The block is set
     * iff at least one of the index's shards has no primary routing (or the index has no shards at all),
     * because the RoutingNodes constructor asserts, per shard, that a non-search-only index has a primary.
     * An index-level block exempts every shard, so a single primary-less shard requires the whole index to
     * be search-only — which is exactly how the two existing single-role builders already behave.
     */
    static void finalizeSearchOnly(IndexShardSummary summary, Settings.Builder settingsBuilder) {
        if (summary.requiresSearchOnly()) {
            settingsBuilder.put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true);
        }
    }

    /**
     * Per-index accumulator over the shards a node contributes for an index (held and/or remote). A
     * non-search-only index must have a primary for EVERY shard, so the index is search-only if any
     * contributed shard lacks a primary, or if the index has no shards.
     */
    static final class IndexShardSummary {
        private boolean hasAnyShard;
        private boolean anyShardLacksPrimary;

        void recordShard(boolean shardHasPrimary) {
            hasAnyShard = true;
            if (shardHasPrimary == false) {
                anyShardLacksPrimary = true;
            }
        }

        boolean requiresSearchOnly() {
            return hasAnyShard == false || anyShardLacksPrimary;
        }
    }
}
