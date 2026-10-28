项目结构：
rocketmq-springboot3-demo/
├── pom.xml
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/example/rocketmq/
│   │   │       ├── RocketmqApplication.java
│   │   │       ├── constant/
│   │   │       │   ├── MQConstant.java
│   │   │       │   └── RedisKeyConstant.java
│   │   │       ├── config/
│   │   │       │   ├── RocketMQConfig.java
│   │   │       │   ├── RedisConfig.java
│   │   │       │   ├── MybatisPlusConfig.java
│   │   │       │   ├── ThreadPoolConfig.java
│   │   │       │   └── JacksonConfig.java
│   │   │       ├── domain/
│   │   │       │   ├── entity/
│   │   │       │   │   ├── Order.java
│   │   │       │   │   └── MessageConsumeLog.java
│   │   │       │   ├── dto/
│   │   │       │   │   └── OrderMessageDTO.java
│   │   │       │   ├── vo/
│   │   │       │   │   └── ResultVO.java
│   │   │       │   └── enums/
│   │   │       │       ├── OrderStatusEnum.java
│   │   │       │       └── ResultCodeEnum.java
│   │   │       ├── producer/
│   │   │       │   └── OrderProducer.java
│   │   │       ├── consumer/
│   │   │       │   ├── OrderNormalConsumer.java
│   │   │       │   ├── OrderOrderlyConsumer.java
│   │   │       │   ├── TransactionConsumer.java
│   │   │       │   └── DeadLetterConsumer.java
│   │   │       ├── transaction/
│   │   │       │   └── OrderTransactionListener.java
│   │   │       ├── service/
│   │   │       │   ├── OrderService.java
│   │   │       │   ├── impl/
│   │   │       │   │   └── OrderServiceImpl.java
│   │   │       │   └── MessageIdempotentService.java
│   │   │       ├── mapper/
│   │   │       │   ├── OrderMapper.java
│   │   │       │   └── MessageLogMapper.java
│   │   │       ├── controller/
│   │   │       │   └── MessageController.java
│   │   │       ├── exception/
│   │   │       │   ├── BusinessException.java
│   │   │       │   └── GlobalExceptionHandler.java
│   │   │       └── util/
│   │   │           ├── TraceIdUtil.java
│   │   │           └── JsonUtil.java
│   │   └── resources/
│   │       ├── application.yml
│   │       ├── application-dev.yml
│   │       ├── application-prod.yml
│   │       ├── application-test.yml
│   │       ├── db/
│   │       │   └── schema.sql
│   │       └── logback-spring.xml
│   └── test/
│       └── java/
│           └── com/example/rocketmq/
│               └── RocketmqTest.java