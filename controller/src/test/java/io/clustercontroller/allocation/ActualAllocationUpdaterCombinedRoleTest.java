package io.clustercontroller.allocation;

import io.clustercontroller.metrics.MetricsProvider;
import io.clustercontroller.models.SearchUnit;
import io.clustercontroller.models.SearchUnitGoalState;
import io.clustercontroller.store.MetadataStore;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests that the coordinator goal-state update also attaches remote_shards to the per-node goal-state of
 * nodes that advertise the coordinates capability (combined data+coordinator role), and only those.
 */
class ActualAllocationUpdaterCombinedRoleTest {

    @Mock
    private MetadataStore metadataStore;

    @Mock
    private MetricsProvider metricsProvider;

    @Mock
    private Counter mockCounter;

    private ActualAllocationUpdater updater;

    private static final String CLUSTER = "test-cluster";

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        updater = new ActualAllocationUpdater(metadataStore, metricsProvider);
        lenient().when(metricsProvider.counter(anyString(), anyMap())).thenReturn(mockCounter);
        // No indices/aliases: the coordinator goal-state is built empty, then propagation runs over the units.
        when(metadataStore.getAllIndexConfigs(CLUSTER)).thenReturn(List.of());
        when(metadataStore.getAllAliases(CLUSTER)).thenReturn(List.of());
    }

    @Test
    void attachesRemoteShardsToCombinedDataNodeOnly() throws Exception {
        SearchUnit combined = unit("data-1", "PRIMARY", true);
        SearchUnit pureData = unit("data-2", "PRIMARY", false);
        SearchUnit coordinator = unit("coordinator-1", "COORDINATOR", true);

        SearchUnitGoalState combinedGoal = new SearchUnitGoalState();
        combinedGoal.setLocalShards(Map.of("idx1", Map.of("0", "PRIMARY")));
        when(metadataStore.getSearchUnitGoalState(CLUSTER, "data-1")).thenReturn(combinedGoal);

        updater.updateCoordinatorGoalStates(CLUSTER, List.of(combined, pureData, coordinator));

        // The combined data node gets remote_shards attached, with its local_shards preserved.
        ArgumentCaptor<SearchUnitGoalState> captor = ArgumentCaptor.forClass(SearchUnitGoalState.class);
        verify(metadataStore).setSearchUnitGoalState(eq(CLUSTER), eq("data-1"), captor.capture());
        assertThat(captor.getValue().getRemoteShards()).isNotNull();
        assertThat(captor.getValue().getLocalShards()).containsKey("idx1");

        // The pure data node and the dedicated coordinator are left untouched.
        verify(metadataStore, never()).setSearchUnitGoalState(eq(CLUSTER), eq("data-2"), any());
        verify(metadataStore, never()).setSearchUnitGoalState(eq(CLUSTER), eq("coordinator-1"), any());
    }

    @Test
    void skipsCombinedNodeWithoutExistingGoalState() throws Exception {
        SearchUnit combined = unit("data-1", "PRIMARY", true);
        when(metadataStore.getSearchUnitGoalState(CLUSTER, "data-1")).thenReturn(null);

        updater.updateCoordinatorGoalStates(CLUSTER, List.of(combined));

        verify(metadataStore, never()).setSearchUnitGoalState(eq(CLUSTER), eq("data-1"), any());
    }

    private static SearchUnit unit(String name, String role, boolean coordinates) {
        SearchUnit unit = new SearchUnit();
        unit.setName(name);
        unit.setRole(role);
        unit.setCoordinates(coordinates);
        return unit;
    }
}
