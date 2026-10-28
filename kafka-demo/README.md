项目结构框架：
kafka-demo/
├── pom.xml
├── src/
│   └── main/
│       ├── java/
│       │   └── com/
│       │       └── example/
│       │           └── kafka/
│       │               ├── KafkaDemoApplication.java          # 启动类
│       │               │
│       │               ├── config/                            # 配置类
│       │               │   ├── KafkaConfig.java               # Kafka核心配置
│       │               │   ├── RedisConfig.java               # Redis配置（幂等）
│       │               │   └── ThreadPoolConfig.java          # 线程池配置
│       │               │
│       │               ├── model/                             # 消息模型
│       │               │   ├── Message.java                   # 通用消息体
│       │               │   ├── OrderMessage.java              # 订单消息
│       │               │   └── UserEvent.java                 # 用户事件
│       │               │
│       │               ├── producer/                          # 生产者
│       │               │   ├── NormalProducer.java            # 普通消息生产者
│       │               │   ├── OrderedProducer.java           # 顺序消息生产者
│       │               │   └── TransactionalProducer.java     # 事务消息生产者
│       │               │
│       │               ├── consumer/                          # 消费者
│       │               │   ├── NormalConsumer.java            # 普通消息消费者
│       │               │   ├── OrderedConsumer.java           # 顺序消息消费者
│       │               │   ├── TransactionalConsumer.java     # 事务消息消费者
│       │               │   └── DeadLetterConsumer.java        # 死信队列消费者
│       │               │
│       │               ├── service/                           # 业务服务
│       │               │   ├── IdempotentService.java         # 幂等服务
│       │               │   ├── OrderService.java              # 订单服务
│       │               │   ├── RetryService.java              # 重试服务
│       │               │   └── DeadLetterService.java         # 死信处理服务
│       │               │
│       │               ├── handler/                           # 处理器
│       │               │   ├── KafkaErrorHandler.java         # 全局异常处理器
│       │               │   ├── CustomPartitioner.java         # 自定义分区器
│       │               │   └── MessageInterceptor.java        # 消息拦截器
│       │               │
│       │               ├── monitor/                           # 监控
│       │               │   ├── KafkaMonitor.java              # Kafka监控
│       │               │   ├── MetricsCollector.java          # 指标收集
│       │               │   └── HealthIndicator.java           # 健康检查
│       │               │
│       │               ├── controller/                        # REST接口
│       │               │   └── KafkaController.java           # 测试接口
│       │               │
│       │               ├── util/                              # 工具类
│       │               │   ├── KafkaUtils.java                # Kafka工具
│       │               │   ├── JsonUtils.java                 # JSON工具
│       │               │   └── TraceIdUtils.java              # 链路追踪
│       │               │
│       │               └── constant/                          # 常量
│       │                   └── KafkaConstants.java            # Kafka常量
│       │
│       └── resources/
│           ├── application.yml                                # 主配置文件
│           ├── application-dev.yml                            # 开发环境配置
│           ├── application-prod.yml                           # 生产环境配置
│           ├── logback-spring.xml                             # 日志配置
│           └── init/
│               └── init-topics.sh                             # 初始化主题脚本
│
└── docker/
├── docker-compose.yml                                     # Docker编排
├── kafka/
│   └── server.properties                                  # Kafka配置
└── redis/
└── redis.conf                                         # Redis配置