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
     * Contributes a shard held locally by this node to the given index builders, reproducing the
     * recovery-source, allocation-id-preservation, peer-wiring, in-sync and primary-term handling of a
     * data node.
     *
     * @param index                    the index the shard belongs to
     * @param dataNodeShard            the locally-held shard to contribute
     * @param localNode               this node, the holder of the shard
     * @param previousIndexRoutingTable the index's routing table from the previous cluster state (may be null)
     * @param indexMetadataBuilder    accumulates primary terms and in-sync allocation ids
     * @param indexRoutingTableBuilder accumulates the shard routing table
     * @param nodesBuilder            accumulates discovery nodes referenced by peer (primary/replica) routings
     * @param settingsBuilder         the index settings builder (search-only is set here for held search replicas)
     */
    static void contributeHeldShard(
        Index index,
        DataNodeShard dataNodeShard,
        DiscoveryNode localNode,
        IndexRoutingTable previousIndexRoutingTable,
        IndexMetadata.Builder indexMetadataBuilder,
        IndexRoutingTable.Builder indexRoutingTableBuilder,
        DiscoveryNodes.Builder nodesBuilder,
        Settings.Builder settingsBuilder
    ) {
        int shardNum = dataNodeShard.getShardNum();
        indexMetadataBuilder.primaryTerm(shardNum, 1);
        ShardRole role = dataNodeShard.getShardRole();
        ShardId shardId = new ShardId(index, shardNum);

        IndexShardRoutingTable.Builder newShardRoutingTable = new IndexShardRoutingTable.Builder(shardId);
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
                newShardRoutingTable.addShard(previousShardRoutingTable.primaryShard());
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
                // Add the primary shard routing to the index routing table
                newShardRoutingTable.addShard(primaryShardRouting);
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
                newShardRoutingTable.addShard(replicaShardRouting);
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
        newShardRoutingTable.addShard(shardRouting);

        indexRoutingTableBuilder.addIndexShard(newShardRoutingTable.build());
        inSyncAllocationIds.add(shardRouting.allocationId().getId());
        indexMetadataBuilder.putInSyncAllocationIds(shardNum, inSyncAllocationIds);
        if (role == ShardRole.SEARCH_REPLICA) {
            // For local search replicas, we have no reference to the primary shard, so we must claim that
            // the index is search-only. Otherwise, an assertion in the RoutingNodes constructor will fail.
            settingsBuilder.put(IndexMetadata.INDEX_BLOCKS_SEARCH_ONLY_SETTING.getKey(), true);
        }
    }

    /**
     * Contributes a shard coordinated for a remote node to the given index routing table, as synthetic
     * STARTED routings pointing at the assigned node ids (the coordinator never holds these shards itself).
     *
     * @param index                    the index the shard belongs to
     * @param shardNum                the shard number being contributed
     * @param shardAssignments        the per-node assignments for this shard
     * @param indexRoutingTableBuilder accumulates the shard routing table
     * @return whether this shard has a primary assigned
     */
    static boolean contributeRemoteShard(
        Index index,
        int shardNum,
        List<NodeShardAssignment> shardAssignments,
        IndexRoutingTable.Builder indexRoutingTableBuilder
    ) {
        ShardId shardId = new ShardId(index, shardNum);
        IndexShardRoutingTable.Builder shardRoutingTableBuilder = new IndexShardRoutingTable.Builder(shardId);
        boolean shardHasPrimary = false;
        for (NodeShardAssignment shardAssignment : shardAssignments) {
            ShardRole shardRole = shardAssignment.shardRole();
            if (shardRole == ShardRole.PRIMARY) {
                shardHasPrimary = true;
            }
            ShardRouting nodeEntry = ShardRouting.newUnassigned(
                shardId,
                shardRole == ShardRole.PRIMARY,
                shardRole == ShardRole.SEARCH_REPLICA,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "initializing")
            );
            nodeEntry = nodeEntry.initialize(shardAssignment.nodeId(), null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
            nodeEntry = nodeEntry.moveToStarted();
            shardRoutingTableBuilder.addShard(nodeEntry);
        }
        indexRoutingTableBuilder.addIndexShard(shardRoutingTableBuilder.build());
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
}
