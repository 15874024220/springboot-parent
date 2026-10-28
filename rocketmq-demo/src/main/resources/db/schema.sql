-- 创建数据库
CREATE DATABASE IF NOT EXISTS rocketmq_demo
DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE rocketmq_demo;

-- 订单表
CREATE TABLE IF NOT EXISTS `order` (
                                       `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
                                       `order_id` VARCHAR(64) NOT NULL COMMENT '订单号（业务唯一键）',
    `status` TINYINT NOT NULL DEFAULT 1 COMMENT '订单状态：1-创建 2-支付 3-完成 4-取消',
    `amount` BIGINT DEFAULT 0 COMMENT '订单金额（分）',
    `user_id` VARCHAR(64) DEFAULT NULL COMMENT '用户ID',
    `content` TEXT COMMENT '订单内容',
    `version` INT DEFAULT 0 COMMENT '乐观锁版本号',
    `deleted` TINYINT DEFAULT 0 COMMENT '逻辑删除：0-未删除，1-已删除',
    `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_order_id` (`order_id`),
    KEY `idx_user_id` (`user_id`),
    KEY `idx_status` (`status`),
    KEY `idx_create_time` (`create_time`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='订单表';

-- 消息消费日志表（用于幂等性记录）
CREATE TABLE IF NOT EXISTS `message_consume_log` (
                                                     `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
                                                     `message_id` VARCHAR(64) NOT NULL COMMENT '消息ID',
    `business_key` VARCHAR(64) NOT NULL COMMENT '业务Key（订单号）',
    `consumer_group` VARCHAR(64) NOT NULL COMMENT '消费者组',
    `status` TINYINT NOT NULL DEFAULT 1 COMMENT '消费状态：1-成功 2-失败',
    `retry_times` INT DEFAULT 0 COMMENT '重试次数',
    `cost_time` INT DEFAULT 0 COMMENT '消费耗时（毫秒）',
    `error_message` TEXT COMMENT '异常信息',
    `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `update_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_msg_biz` (`message_id`, `business_key`),
    KEY `idx_create_time` (`create_time`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='消息消费日志表';

-- 死信消息表（用于记录死信消息，人工处理）
CREATE TABLE IF NOT EXISTS `dead_letter_message` (
                                                     `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键ID',
                                                     `message_id` VARCHAR(64) NOT NULL COMMENT '消息ID',
    `order_id` VARCHAR(64) NOT NULL COMMENT '订单号',
    `content` TEXT COMMENT '消息内容',
    `status` TINYINT DEFAULT 0 COMMENT '处理状态：0-待处理，1-已处理',
    `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `process_time` DATETIME COMMENT '处理时间',
    `process_remark` VARCHAR(500) COMMENT '处理备注',
    PRIMARY KEY (`id`),
    KEY `idx_order_id` (`order_id`),
    KEY `idx_status` (`status`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='死信消息表';