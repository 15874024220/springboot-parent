package com.example.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Kafka Demo 启动类
 *
 * @EnableKafka: 启用Kafka监听器注解
 * @EnableScheduling: 启用定时任务（监控）
 */
@Slf4j
@EnableKafka
@EnableScheduling
@SpringBootApplication
public class KafkaDemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDemoApplication.class, args);
        log.info("Kafka Demo 启动成功！");
    }
}