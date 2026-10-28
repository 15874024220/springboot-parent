package com.example.rocketmq;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * SpringBoot 启动类
 *
 */
@SpringBootApplication
@EnableScheduling  // 启用定时任务
public class RocketmqApplication {

    public static void main(String[] args) {
        SpringApplication.run(RocketmqApplication.class, args);
        System.out.println("========================================");
        System.out.println("RocketMQ SpringBoot 3 Demo Started!");
        System.out.println("API Base URL: http://localhost:8080/api/message");
        System.out.println("========================================");
    }
}