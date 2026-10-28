package com.example.kafka.controller;

import com.example.kafka.model.Message;
import com.example.kafka.model.OrderMessage;
import com.example.kafka.producer.NormalProducer;
import com.example.kafka.producer.OrderedProducer;
import com.example.kafka.producer.TransactionalProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Kafka 测试接口
 */
@Slf4j
@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
public class KafkaController {

    private final NormalProducer normalProducer;
    private final OrderedProducer orderedProducer;
    private final TransactionalProducer transactionalProducer;

    /**
     * 发送普通消息
     * GET /api/kafka/send/normal?content=hello
     */
    @GetMapping("/send/normal")
    public Map<String, Object> sendNormalMessage(@RequestParam String content) {
        Map<String, Object> result = new HashMap<>();

        Message<String> message = Message.<String>builder()
                .messageId(UUID.randomUUID().toString())
                .businessId("business-" + System.currentTimeMillis())
                .payload(content)
                .source("rest-api")
                .build();

        normalProducer.sendAsync(message);

        result.put("code", 200);
        result.put("message", "发送成功");
        result.put("messageId", message.getMessageId());
        return result;
    }

    /**
     * 发送顺序消息
     * GET /api/kafka/send/ordered?businessId=order_001&content=create_order
     */
    @GetMapping("/send/ordered")
    public Map<String, Object> sendOrderedMessage(
            @RequestParam String businessId,
            @RequestParam String content) {
        Map<String, Object> result = new HashMap<>();

        Map<String, Object> payload = new HashMap<>();
        payload.put("action", content);
        payload.put("timestamp", System.currentTimeMillis());

        orderedProducer.sendOrderedMessage(businessId, payload);

        result.put("code", 200);
        result.put("message", "顺序消息发送成功");
        result.put("businessId", businessId);
        return result;
    }

    /**
     * 发送订单消息（演示业务场景）
     * POST /api/kafka/order
     */
    @PostMapping("/order")
    public Map<String, Object> sendOrderMessage(@RequestBody OrderMessage order) {
        Map<String, Object> result = new HashMap<>();

        order.setOrderId(UUID.randomUUID().toString());

        transactionalProducer.sendWithTransaction(order.getOrderId(), order);

        result.put("code", 200);
        result.put("message", "订单消息发送成功");
        result.put("orderId", order.getOrderId());
        return result;
    }

    /**
     * 发送订单状态更新（顺序消息示例）
     * PUT /api/kafka/order/{orderId}/status
     */
    @PutMapping("/order/{orderId}/status")
    public Map<String, Object> updateOrderStatus(
            @PathVariable String orderId,
            @RequestParam Integer status) {
        Map<String, Object> result = new HashMap<>();

        Map<String, Object> payload = new HashMap<>();
        payload.put("orderId", orderId);
        payload.put("status", status);
        payload.put("action", "update-status");

        // 使用 orderId 作为 businessId，确保同一订单的状态更新按顺序处理
        orderedProducer.sendOrderedMessage(orderId, payload);

        result.put("code", 200);
        result.put("message", "订单状态更新消息已发送");
        result.put("orderId", orderId);
        return result;
    }

    /**
     * 健康检查
     * GET /api/kafka/health
     */
    @GetMapping("/health")
    public Map<String, Object> healthCheck() {
        Map<String, Object> result = new HashMap<>();
        result.put("status", "UP");
        result.put("service", "kafka-demo");
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
}