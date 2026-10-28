package com.example.rocketmq.config;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.apache.rocketmq.spring.support.RocketMQMessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * RocketMQ核心配置类
 *
 * 功能说明：
 * 1. 配置自定义生产者（DefaultMQProducer）：用于精细控制生产者行为
 * 2. 配置自定义消费者（DefaultMQPushConsumer）：用于精细控制消费者行为
 * 3. 配置 RocketMQTemplate：Spring Boot 集成核心组件
 *
 * 注意：
 * - 消息轨迹（enableMsgTrace）需要通过 RocketMQTemplate 配置，而非原生 Producer/Consumer
 * - 如需使用消息轨迹，请在 application.yml 中配置 rocketmq.producer.enable-msg-trace
 *
 * @author example
 * @date 2024
 */
@Slf4j
@Configuration
public class RocketMQConfig {

    // ==================== 生产者配置 ====================
    @Value("${rocketmq.name-server}")
    private String nameServer;

    @Value("${rocketmq.producer.group}")
    private String producerGroup;

    @Value("${rocketmq.producer.send-message-timeout:3000}")
    private Integer sendMsgTimeout;

    @Value("${rocketmq.producer.max-message-size:4194304}")
    private Integer maxMessageSize;

    @Value("${rocketmq.producer.retry-times-when-send-failed:2}")
    private Integer retryTimesWhenSendFailed;

    @Value("${rocketmq.producer.retry-times-when-send-async-failed:2}")
    private Integer retryTimesWhenSendAsyncFailed;

    @Value("${rocketmq.producer.vip-channel-enabled:false}")
    private boolean vipChannelEnabled;

    // ==================== 消费者配置 ====================
    @Value("${rocketmq.consumer.consume-thread-min:5}")
    private Integer consumeThreadMin;

    @Value("${rocketmq.consumer.consume-thread-max:20}")
    private Integer consumeThreadMax;

    @Value("${rocketmq.consumer.pull-batch-size:32}")
    private Integer pullBatchSize;

    @Value("${rocketmq.consumer.consume-timeout:15}")
    private Integer consumeTimeout;

    @Value("${rocketmq.consumer.max-reconsume-times:3}")
    private Integer maxReconsumeTimes;

    /**
     * 配置自定义生产者 Bean
     *
     * 适用场景：
     * - 需要精细控制生产者参数
     * - 需要使用多个不同的生产者实例
     * - 需要自定义发送钩子
     *
     * 注意：
     * - 如果同时存在 RocketMQTemplate 和自定义生产者，建议使用 @Primary 指定主生产者
     * - destroyMethod = "shutdown" 确保应用关闭时正确释放资源
     *
     * @return DefaultMQProducer 生产者实例
     */
//    @Bean(name = "customRocketMQProducer", destroyMethod = "shutdown")
//    @Primary
    public DefaultMQProducer defaultMQProducer() {
        log.info("初始化自定义RocketMQ生产者 - Group: {}, NameServer: {}", producerGroup, nameServer);

        // 1. 创建生产者实例
        DefaultMQProducer producer = new DefaultMQProducer();

        // 2. 设置生产者组名（必需，用于事务消息和生产者集群）
        producer.setProducerGroup(producerGroup);

        // 3. 设置NameServer地址（必需，支持多个地址用分号分隔）
        producer.setNamesrvAddr(nameServer);

        // 4. 设置发送超时时间（毫秒）
        //    建议值：3000-5000ms，根据网络状况调整
        producer.setSendMsgTimeout(sendMsgTimeout);

        // 5. 设置最大消息大小（字节）
        //    默认4M，超过会抛出异常，需根据业务调整
        producer.setMaxMessageSize(maxMessageSize);

        // 6. 设置同步发送失败重试次数
        //    注意：此重试是客户端重试，不包括Broker端重试
        producer.setRetryTimesWhenSendFailed(retryTimesWhenSendFailed);

        // 7. 设置异步发送失败重试次数
        producer.setRetryTimesWhenSendAsyncFailed(retryTimesWhenSendAsyncFailed);

        // 8. 设置是否启用VIP通道
        //    VIP通道拥有更高优先级，但可能在某些网络环境下有问题
        //    生产环境建议设置为false
        producer.setVipChannelEnabled(vipChannelEnabled);

        // 9. 设置压缩消息体阈值（字节）
        //    超过此大小的消息会使用zip压缩，减少网络传输
        producer.setCompressMsgBodyOverHowmuch(4096);

        // 10. 设置发送消息超时监听器（可选）
        //    用于监听发送超时事件，便于问题排查
        // producer.setSendMessageTimeoutListener(timeout -> {
        //     log.warn("消息发送超时: {}ms", timeout);
        // });

        // 11. 设置回调线程池大小（可选）
        //    用于异步发送的回调处理
        // producer.setCallbackExecutor(threadPoolExecutor);

        // 12. 启动生产者（必需）
        try {
            producer.start();
            log.info("RocketMQ生产者启动成功 - Group: {}", producerGroup);
        } catch (MQClientException e) {
            log.error("RocketMQ生产者启动失败 - Group: {}", producerGroup, e);
            throw new RuntimeException("RocketMQ生产者启动失败: " + e.getErrorMessage(), e);
        }

        return producer;
    }

    /**
     * 配置 RocketMQTemplate（核心组件）
     *
     * 功能说明：
     * 1. RocketMQTemplate 是 Spring Boot 集成 RocketMQ 的核心组件
     * 2. 用于发送消息（同步、异步、顺序、事务等）
     * 3. 需要注入自定义的 Producer
     *
     * 注意：
     * - 如果不配置此 Bean，Spring Boot Starter 会自动配置一个 RocketMQTemplate
     * - 但自动配置的 RocketMQTemplate 可能无法使用自定义的 Producer
     * - 因此建议手动配置，确保使用自定义的 Producer
     *
     * @param customProducer 自定义生产者
     * @param messageConverter 消息转换器（自动注入）
     * @return RocketMQTemplate
     */
//    @Bean
//    @Primary  // 标记为主实例，覆盖自动配置的 RocketMQTemplate
    public RocketMQTemplate rocketMQTemplate(
            DefaultMQProducer customProducer,
            RocketMQMessageConverter messageConverter) {

        log.info("初始化RocketMQTemplate - 使用自定义Producer: {}", producerGroup);

        RocketMQTemplate template = new RocketMQTemplate();

        // 关键步骤：将自定义的 Producer 设置到 RocketMQTemplate 中
        // 如果不设置，RocketMQTemplate 内部的 producer 为 null，调用时会抛出 NPE
        template.setProducer(customProducer);

        // 设置消息转换器（用于对象和JSON之间的转换）
        template.setMessageConverter(messageConverter.getMessageConverter());

        log.info("RocketMQTemplate初始化完成");

        return template;
    }

    /**
     * 配置自定义消费者 Bean（备选方案）
     *
     * 适用场景：
     * - 需要动态订阅Topic（@RocketMQMessageListener是静态订阅）
     * - 需要精细控制消费者参数
     * - 需要手动管理消费者生命周期
     *
     * 注意：
     * - 通常使用 @RocketMQMessageListener 注解即可，更简单易用
     * - 使用手动配置时，需要自己管理消费者的启动和关闭
     * - 同一ConsumerGroup下的订阅必须完全一致
     *
     * @return DefaultMQPushConsumer 消费者实例
     * @throws MQClientException 客户端异常
     */
//    @Bean(name = "customRocketMQConsumer", destroyMethod = "shutdown")
    public DefaultMQPushConsumer defaultMQPushConsumer() throws MQClientException {
        log.info("初始化自定义RocketMQ消费者 - NameServer: {}", nameServer);

        // 1. 创建消费者实例
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();

        // 2. 设置消费者组名（必需）
        //    同一ConsumerGroup内的消费者共同消费消息
        // consumer.setConsumerGroup("custom_consumer_group");

        // 3. 设置NameServer地址（必需）
        consumer.setNamesrvAddr(nameServer);

        // 4. 设置从何处开始消费
        //    - CONSUME_FROM_LAST_OFFSET: 从最新开始（默认）
        //    - CONSUME_FROM_FIRST_OFFSET: 从头开始
        //    - CONSUME_FROM_TIMESTAMP: 从指定时间戳开始
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        // 5. 设置消费线程池大小
        //    - consumeThreadMin: 最小线程数（默认10）
        //    - consumeThreadMax: 最大线程数（默认20）
        //    根据业务处理速度和CPU核数调整
        consumer.setConsumeThreadMin(consumeThreadMin);
        consumer.setConsumeThreadMax(consumeThreadMax);

        // 6. 设置拉取消息间隔（毫秒）
        //    默认0，表示有消息时立即拉取
        consumer.setPullInterval(0);

        // 7. 设置单次拉取消息数量
        //    默认32，最大32，影响消费吞吐量
        consumer.setPullBatchSize(pullBatchSize);

        // 8. 设置消费超时时间（分钟）
        //    如果超过此时间未确认消费完成，消息会被重新投递
        //    默认15分钟，需根据业务处理时间调整
        consumer.setConsumeTimeout(consumeTimeout);

        // 9. 设置最大重试次数
        //    默认16次，超过后进入死信队列（%DLQ%{consumerGroup}）
        consumer.setMaxReconsumeTimes(maxReconsumeTimes);

        // 10. 设置消息模型
        //     - CLUSTERING（集群模式）: 同一ConsumerGroup内的消费者分摊消息
        //     - BROADCASTING（广播模式）: 每个消费者都会收到所有消息
        consumer.setMessageModel(MessageModel.CLUSTERING);

        // 11. 设置消息监听器（需要手动实现）
        //     consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
        //         for (MessageExt msg : msgs) {
        //             log.info("收到消息: {}", new String(msg.getBody()));
        //         }
        //         return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        //     });

        // 注意：需要手动设置订阅的Topic和Tag
        // consumer.subscribe("order_topic", "*");

        log.info("RocketMQ消费者配置完成 - NameServer: {}", nameServer);

        // 注意：不要在这里启动消费者，应该在设置好订阅和监听器后再启动
        // consumer.start();

        return consumer;
    }

    /**
     * 初始化配置日志输出
     */
    @PostConstruct
    public void init() {
        log.info("========== RocketMQ配置初始化完成 ==========");
        log.info("NameServer: {}", nameServer);
        log.info("生产者组: {}", producerGroup);
        log.info("生产者配置 - 发送超时: {}ms, 最大消息: {}B, 重试次数: {}",
                sendMsgTimeout, maxMessageSize, retryTimesWhenSendFailed);
        log.info("消费者配置 - 线程数: {}/{}, 拉取批量: {}, 超时: {}min, 最大重试: {}",
                consumeThreadMin, consumeThreadMax, pullBatchSize, consumeTimeout, maxReconsumeTimes);
        log.info("============================================");
    }

    /**
     * 优雅关闭钩子
     * 确保应用关闭时消息发送和消费能正确结束
     */
    @PreDestroy
    public void destroy() {
        log.info("开始优雅关闭RocketMQ资源");
        // 注意：DefaultMQProducer 和 DefaultMQPushConsumer 已经通过 destroyMethod = "shutdown" 自动关闭
        // 这里不需要重复关闭，仅做日志记录
        log.info("RocketMQ资源关闭完成");
    }
}