package io.clustercontroller.models;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SearchUnitGoalStateTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void omitsRemoteShardsForPureDataNode() throws Exception {
        SearchUnitGoalState goalState = new SearchUnitGoalState();
        goalState.setLocalShards(Map.of("idx1", Map.of("0", "PRIMARY")));

        String json = objectMapper.writeValueAsString(goalState);

        // A pure data node's document must not gain a remote_shards key.
        assertThat(json).doesNotContain("remote_shards");
        assertThat(json).contains("local_shards");
    }

    @Test
    void includesRemoteShardsForCombinedNodeAndRoundTrips() throws Exception {
        SearchUnitGoalState goalState = new SearchUnitGoalState();
        goalState.setLocalShards(Map.of("idx1", Map.of("0", "PRIMARY")));
        goalState.setRemoteShards(new CoordinatorGoalState.RemoteShards());

        String json = objectMapper.writeValueAsString(goalState);
        assertThat(json).contains("remote_shards");

        SearchUnitGoalState parsed = objectMapper.readValue(json, SearchUnitGoalState.class);
        assertThat(parsed.getRemoteShards()).isNotNull();
        assertThat(parsed.getLocalShards()).containsKey("idx1");
    }
}
