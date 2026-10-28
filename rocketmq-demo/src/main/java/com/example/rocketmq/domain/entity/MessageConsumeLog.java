package com.example.rocketmq.domain.entity;

import com.baomidou.mybatisplus.annotation.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDateTime;

/**
 * 消息消费日志实体类
 *
 * 功能：
 * 1. 记录消息消费历史，支持幂等性校验
 * 2. 记录消费失败原因，便于问题排查
 * 3. 支持死信消息追踪
 * 4. 提供消费监控数据
 *
 * @author example
 * @date 2024
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@TableName("message_consume_log")
public class MessageConsumeLog implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 主键ID（自增）
     */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /**
     * 消息ID（全局唯一）
     * 由生产者生成，用于幂等性校验
     */
    @TableField("message_id")
    private String messageId;

    /**
     * 业务Key（如订单号）
     * 用于关联业务数据
     */
    @TableField("business_key")
    private String businessKey;

    /**
     * Topic名称
     */
    @TableField("topic")
    private String topic;

    /**
     * Tag标签
     */
    @TableField("tag")
    private String tag;

    /**
     * 消费者组名
     */
    @TableField("consumer_group")
    private String consumerGroup;

    /**
     * 消费状态
     * 1-成功，2-失败，3-重试中，4-进入死信队列
     */
    @TableField("status")
    private Integer status;

    /**
     * 重试次数
     */
    @TableField("retry_times")
    private Integer retryTimes;

    /**
     * 消费耗时（毫秒）
     */
    @TableField("cost_time")
    private Integer costTime;

    /**
     * 消费开始时间
     */
    @TableField("start_time")
    private LocalDateTime startTime;

    /**
     * 消费结束时间
     */
    @TableField("end_time")
    private LocalDateTime endTime;

    /**
     * 异常信息（失败时记录）
     */
    @TableField("error_message")
    private String errorMessage;

    /**
     * 异常堆栈（可选，生产环境建议只记录摘要）
     */
    @TableField("error_stack")
    private String errorStack;

    /**
     * 消息内容（JSON格式，可选，用于问题排查）
     */
    @TableField("message_content")
    private String messageContent;

    /**
     * 消费者IP地址
     */
    @TableField("consumer_ip")
    private String consumerIp;

    /**
     * 创建时间
     */
    @TableField(value = "create_time", fill = FieldFill.INSERT)
    private LocalDateTime createTime;

    /**
     * 更新时间
     */
    @TableField(value = "update_time", fill = FieldFill.INSERT_UPDATE)
    private LocalDateTime updateTime;

    // ==================== 状态常量 ====================

    /**
     * 消费状态枚举
     */
    public static class Status {
        public static final int SUCCESS = 1;      // 成功
        public static final int FAILED = 2;       // 失败
        public static final int RETRYING = 3;     // 重试中
        public static final int DEAD_LETTER = 4;  // 进入死信队列
    }
}