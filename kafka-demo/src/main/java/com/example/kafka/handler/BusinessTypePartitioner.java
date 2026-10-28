package com.example.kafka.handler;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按业务属性分区的增强分区器
 *
 * 适用场景：
 * - 需要将特定业务类型的数据路由到指定分区
 * - 需要灰度发布、流量隔离
 *
 */
public class BusinessTypePartitioner implements Partitioner {

    // 业务类型到分区的映射（可配置化）
    private final Map<String, Integer> businessTypePartitionMap = new ConcurrentHashMap<>();

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        int partitionCount = cluster.partitionCountForTopic(topic);

        // 方案1：从 Key 中解析业务类型
        if (key != null) {
            String keyStr = key.toString();
            String businessType = extractBusinessType(keyStr);

            // 检查是否有预留分区
            if (businessTypePartitionMap.containsKey(businessType)) {
                return businessTypePartitionMap.get(businessType);
            }

            // 特殊业务路由逻辑
            if ("VIP".equals(businessType)) {
                // VIP 用户消息优先路由到 0 号分区（可单独扩容）
                return 0;
            }

            if ("ORDER".equals(businessType)) {
                // 订单消息按订单号哈希分布
                return (Utils.murmur2(keyBytes) & Integer.MAX_VALUE) % partitionCount;
            }
        }

        // 方案2：从消息体中解析业务字段
        if (value != null && value instanceof String) {
            String valueStr = (String) value;
            if (valueStr.contains("\"payment\"")) {
                // 支付类消息路由到高吞吐分区
                return 1;
            }
        }

        // 默认：按 Key 哈希
        if (keyBytes != null) {
            return (Utils.murmur2(keyBytes) & Integer.MAX_VALUE) % partitionCount;
        }

        // 无 Key：轮询
        return (int)(System.currentTimeMillis() % partitionCount);
    }

    /**
     * 从 Key 中提取业务类型
     * 假设 Key 格式：业务类型_业务ID，如 ORDER_123456
     */
    private String extractBusinessType(String key) {
        if (key == null) return "UNKNOWN";
        int underscoreIndex = key.indexOf('_');
        if (underscoreIndex > 0) {
            return key.substring(0, underscoreIndex);
        }
        return "UNKNOWN";
    }

    @Override
    public void close() {}

    @Override
    public void configure(Map<String, ?> configs) {
        // 从配置加载映射关系
        // configs.get("business.type.partition.0") = "VIP"
        // configs.get("business.type.partition.1") = "ORDER"
    }
}