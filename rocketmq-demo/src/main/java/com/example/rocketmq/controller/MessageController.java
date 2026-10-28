package com.example.rocketmq.controller;

import cn.hutool.core.util.IdUtil;
import com.example.rocketmq.domain.dto.OrderMessageDTO;
import com.example.rocketmq.domain.vo.ResultVO;
import com.example.rocketmq.producer.OrderProducer;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;

/**
 * 消息发送测试Controller
 *
 * 提供REST API用于测试各种消息发送方式
 *
 * @author example
 * 
 */
@Slf4j
@RestController
@RequestMapping("/api/message")
@RequiredArgsConstructor
public class MessageController {

    private final OrderProducer orderProducer;

    /**
     * 发送同步消息
     *
     * @param messageDTO 消息内容
     * @return 发送结果
     */
    @PostMapping("/sync")
    public ResultVO<SendResult> sendSync(@Valid @RequestBody OrderMessageDTO messageDTO) {
        log.info("接收同步消息请求 - OrderId: {}", messageDTO.getOrderId());

        try {
            if (messageDTO.getMessageId() == null) {
                messageDTO.setMessageId(IdUtil.fastSimpleUUID());
            }
            SendResult sendResult = orderProducer.syncSend(messageDTO);
            return ResultVO.success(sendResult);
        } catch (Exception e) {
            log.error("发送失败", e);
            return ResultVO.error("发送失败: " + e.getMessage());
        }
    }

    /**
     * 发送顺序消息
     *
     * @param messageDTO 消息内容
     * @param hashKey 顺序Key（相同Key的消息顺序消费）
     * @return 发送结果
     */
    @PostMapping("/orderly")
    public ResultVO<SendResult> sendOrderly(@Valid @RequestBody OrderMessageDTO messageDTO,
                                            @RequestParam String hashKey) {
        log.info("接收顺序消息请求 - OrderId: {}, HashKey: {}", messageDTO.getOrderId(), hashKey);

        try {
            SendResult sendResult = orderProducer.syncSendOrderly(messageDTO, hashKey);
            return ResultVO.success(sendResult);
        } catch (Exception e) {
            log.error("发送失败", e);
            return ResultVO.error("发送失败: " + e.getMessage());
        }
    }

    /**
     * 发送事务消息
     *
     * @param messageDTO 消息内容
     * @return 发送结果
     */
    @PostMapping("/transaction")
    public ResultVO<Void> sendTransaction(@Valid @RequestBody OrderMessageDTO messageDTO) {
        log.info("接收事务消息请求 - OrderId: {}", messageDTO.getOrderId());

        try {
            orderProducer.sendTransactionMsg(messageDTO);
            return ResultVO.success("事务消息已发送");
        } catch (Exception e) {
            log.error("发送失败", e);
            return ResultVO.error("发送失败: " + e.getMessage());
        }
    }

    /**
     * 批量发送消息
     *
     * @param count 发送数量
     * @return 发送结果
     */
    @PostMapping("/batch")
    public ResultVO<Void> sendBatch(@RequestParam(defaultValue = "10") int count) {
        log.info("接收批量发送请求 - 数量: {}", count);

        List<OrderMessageDTO> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            OrderMessageDTO dto = OrderMessageDTO.builder()
                    .orderId("ORDER_" + System.currentTimeMillis() + "_" + i)
                    .status(1)
                    .amount(1000L)
                    .userId("user_" + i)
                    .content("批量消息_" + i)
                    .build();
            list.add(dto);
        }

        orderProducer.batchSend(list);
        return ResultVO.success("批量发送中，数量: " + count);
    }

    /**
     * 发送测试消息（快速测试）
     *
     * @return 发送结果
     */
    @PostMapping("/test")
    public ResultVO<SendResult> sendTest() {
        String orderId = "TEST_" + System.currentTimeMillis();

        OrderMessageDTO dto = OrderMessageDTO.builder()
                .orderId(orderId)
                .status(1)
                .amount(10000L)
                .userId("test_user")
                .content("测试消息")
                .build();

        return sendSync(dto);
    }
}