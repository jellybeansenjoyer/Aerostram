package com.aerostream.kafka;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TelemetryPartitionerTest {

    private final TelemetryPartitioner partitioner = new TelemetryPartitioner();

    private Cluster clusterWithPartitions(int count) {
        Cluster cluster = mock(Cluster.class);
        List<PartitionInfo> partitions = IntStream.range(0, count)
            .mapToObj(i -> new PartitionInfo("raw-telemetry", i, null, new Node[0], new Node[0]))
            .collect(Collectors.toList());
        when(cluster.partitionsForTopic("raw-telemetry")).thenReturn(partitions);
        return cluster;
    }

    @Test
    void sameCarIdAlwaysMapsToSamePartition() {
        Cluster cluster = clusterWithPartitions(20);
        int first = partitioner.partition("raw-telemetry", "CAR_01", null, null, null, cluster);
        for (int i = 0; i < 100; i++) {
            assertThat(partitioner.partition("raw-telemetry", "CAR_01", null, null, null, cluster))
                .isEqualTo(first);
        }
    }

    @Test
    void allTwentyCarsHaveStablePartitions() {
        Cluster cluster = clusterWithPartitions(20);
        // Each car gets a deterministic partition; verify stability over 10 calls each
        for (int car = 1; car <= 20; car++) {
            String carId = String.format("CAR_%02d", car);
            int expected = partitioner.partition("raw-telemetry", carId, null, null, null, cluster);
            for (int repeat = 0; repeat < 10; repeat++) {
                assertThat(partitioner.partition("raw-telemetry", carId, null, null, null, cluster))
                    .as("Car %s must always land on the same partition", carId)
                    .isEqualTo(expected);
            }
        }
    }

    @Test
    void nullKeyReturnsValidPartition() {
        Cluster cluster = clusterWithPartitions(20);
        int p = partitioner.partition("raw-telemetry", null, null, null, null, cluster);
        assertThat(p).isBetween(0, 19);
    }

    @Test
    void partitionIsWithinBounds() {
        Cluster cluster = clusterWithPartitions(20);
        for (int car = 1; car <= 20; car++) {
            String carId = String.format("CAR_%02d", car);
            int p = partitioner.partition("raw-telemetry", carId, null, null, null, cluster);
            assertThat(p).isBetween(0, 19);
        }
    }
}
