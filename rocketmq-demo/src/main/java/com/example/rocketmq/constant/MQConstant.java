package com.example.rocketmq.constant;

/**
 * RocketMQ常量定义类
 *
 * 集中管理所有MQ相关的常量，便于维护和修改
 *
 * @author example
 * 
 */
public class MQConstant {

    // ==================== Topic定义 ====================
    /** 订单主题 */
    public static final String ORDER_TOPIC = "order_topic";

    /** 消息轨迹主题 */
    public static final String TRACE_TOPIC = "rmq_trace_topic";

    // ==================== Tag定义 ====================
    /** 普通消息标签 - 用于普通订单消息 */
    public static final String TAG_NORMAL = "normal";

    /** 顺序消息标签 - 用于需要保证顺序的订单状态变更 */
    public static final String TAG_ORDERLY = "orderly";

    /** 事务消息标签 - 用于分布式事务场景 */
    public static final String TAG_TRANSACTION = "transaction";

    // ==================== Consumer Group定义 ====================
    /** 普通消息消费者组 */
    public static final String NORMAL_CONSUMER_GROUP = "normal_consumer_group";

    /** 顺序消息消费者组 */
    public static final String ORDERLY_CONSUMER_GROUP = "orderly_consumer_group";

    /** 事务消息消费者组 */
    public static final String TRANSACTION_CONSUMER_GROUP = "transaction_consumer_group";

    /** 死信队列消费者组 */
    public static final String DLQ_CONSUMER_GROUP = "dlq_consumer_group";

    // ==================== 死信队列前缀 ====================
    /**
     * 死信队列前缀
     * 格式：%DLQ%{原消费者组名}
     * 当消息重试次数超过maxReconsumeTimes后，会进入死信队列
     */
    public static final String DLQ_PREFIX = "%DLQ%";

    // ==================== 重试配置 ====================
    /** 最大重试次数（超过进入死信队列） */
    public static final int MAX_RECONSUME_TIMES = 3;

    // ==================== 消息头定义 ====================
    /** 业务Key消息头 - 用于消息查询和顺序消费 */
    public static final String KEYS_HEADER = "KEYS";

    /** 消息ID消息头 - 用于幂等性校验 */
    public static final String MESSAGE_ID_HEADER = "messageId";

    /** 重试次数消息头 */
    public static final String RETRY_TIMES_HEADER = "RETRY_TIMES";
}