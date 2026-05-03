package com.aerostream.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Custom Kafka partitioner that maps each car_id to a deterministic partition.
 *
 * All telemetry events for the same car are guaranteed to land on the same
 * partition, enabling ordered Kafka Streams windowed aggregations in Phase 3
 * without co-partitioning complexity.
 *
 * Partition selection: Math.abs(carId.hashCode()) % numPartitions
 * This is stable across JVM restarts because String.hashCode() is deterministic.
 */
public class TelemetryPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (numPartitions == 0) return 0;
        if (key == null) return ThreadLocalRandom.current().nextInt(numPartitions);

        // Stable hash — same car_id always maps to the same partition
        return Math.abs(key.hashCode()) % numPartitions;
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {}
}
