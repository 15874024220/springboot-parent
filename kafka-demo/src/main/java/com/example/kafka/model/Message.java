package com.example.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * 消息体（支持泛型）
 * 包含消息唯一标识，用于幂等处理
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Message<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 消息唯一ID（用于幂等和去重）
     */
    @Builder.Default
    private String messageId = UUID.randomUUID().toString();

    /**
     * 消息键（用于分区路由，顺序消息的关键）
     */
    private String key;

    /**
     * 消息内容
     */
    private T payload;

    /**
     * 消息时间戳
     */
    @Builder.Default
    private LocalDateTime timestamp = LocalDateTime.now();

    /**
     * 重试次数
     */
    @Builder.Default
    private Integer retryCount = 0;

    /**
     * 消息来源
     */
    private String source;

    /**
     * 业务ID（用于幂等）
     */
    private String businessId;
}
