package com.example.kafka.handler;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义 Kafka 分区器
 *
 * 功能说明：
 * 1. 根据业务 Key 进行哈希分区（保证相同 Key 进入同一分区，保证顺序）
 * 2. 支持粘性分区（Sticky Partitioning）优化小消息场景
 * 3. 支持动态感知分区数量变化
 *
 */
public class CustomPartitioner implements Partitioner {

    // 用于轮询策略的计数器（仅在无 Key 时使用）
    private final AtomicInteger counter = new AtomicInteger(0);

    // 缓存分区数量（避免频繁查询）
    private volatile int cachedPartitionCount = -1;
    private volatile long lastCacheTime = 0;
    private static final long CACHE_TTL_MS = 60000; // 缓存有效期60秒

    /**
     * 核心分区方法
     *
     * @param topic     主题名称
     * @param key       消息的 Key（业务标识）
     * @param keyBytes  Key 序列化后的字节数组
     * @param value     消息体
     * @param valueBytes 消息体序列化后的字节数组
     * @param cluster   集群元数据（可获取分区信息）
     * @return 分区编号（0 ~ partitionCount-1）
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        // 1. 获取主题的分区数量（带缓存）
        int partitionCount = getPartitionCountWithCache(topic, cluster);

        if (partitionCount <= 0) {
            return 0; // 降级策略
        }

        // 2. 优先级1：显式指定分区（通过消息头传递）
        // 此场景需配合 ProducerInterceptor 实现
        if (key != null && key.toString().startsWith("FIXED:")) {
            int fixedPartition = Integer.parseInt(key.toString().substring(6));
            return Math.min(fixedPartition, partitionCount - 1);
        }

        // 3. 优先级2：业务 Key 哈希分区（保证相同 Key 进入同一分区，有序消费）
        if (keyBytes != null && key != null) {
            // 使用 Kafka 内置的 murmur2 哈希算法，保证均匀分布
            int hash = Utils.murmur2(keyBytes);
            return (hash & Integer.MAX_VALUE) % partitionCount;
        }

        // 4. 优先级3：无 Key 时采用粘性分区（Sticky Partitioner）
        // Kafka 2.4+ 默认使用 UniformStickyPartitioner，性能优于轮询
        // 实现思路：维护当前分区的使用计数，达到阈值后切换
        return stickyPartition(partitionCount);
    }

    /**
     * 带缓存的获取分区数量（避免频繁调用 cluster.partitionCountForTopic）
     */
    private int getPartitionCountWithCache(String topic, Cluster cluster) {
        long now = System.currentTimeMillis();
        if (cachedPartitionCount == -1 || (now - lastCacheTime) > CACHE_TTL_MS) {
            cachedPartitionCount = cluster.partitionCountForTopic(topic);
            lastCacheTime = now;
        }
        return cachedPartitionCount;
    }

    /**
     * 粘性分区策略
     *
     * 思路：消息先填满当前分区的批次，再切换到下一个分区
     * 实际生产环境推荐直接使用 Kafka 内置的 UniformStickyPartitioner
     */
    private int stickyPartition(int partitionCount) {
        // 简化实现：轮询 + 随机偏移
        int current = counter.getAndIncrement();
        return (current % partitionCount + (int)(System.currentTimeMillis() % partitionCount)) % partitionCount;
    }

    @Override
    public void close() {
        // 释放资源（本实现无需特殊处理）
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // 从配置中读取自定义参数
        // configs.get("custom.partitioner.param")
    }
}