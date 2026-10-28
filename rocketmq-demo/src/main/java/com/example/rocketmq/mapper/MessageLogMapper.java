package com.example.rocketmq.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.example.rocketmq.domain.entity.MessageConsumeLog;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * 消息消费日志Mapper接口
 *
 * 用于记录消息消费情况，支持幂等性校验和问题排查
 *
 * @author example
 * 
 */
@Mapper
public interface MessageLogMapper extends BaseMapper<MessageConsumeLog> {

    /**
     * 根据消息ID和业务Key查询消费记录
     *
     * @param messageId 消息ID
     * @param businessKey 业务Key
     * @return 消费记录
     */
    @Select("SELECT * FROM message_consume_log " +
            "WHERE message_id = #{messageId} AND business_key = #{businessKey}")
    MessageConsumeLog selectByMessageIdAndBusinessKey(
            @Param("messageId") String messageId,
            @Param("businessKey") String businessKey);

    /**
     * 查询失败次数超过阈值的消息
     *
     * @param maxRetryTimes 最大重试次数阈值
     * @return 失败消息列表
     */
    @Select("SELECT * FROM message_consume_log " +
            "WHERE status = 2 AND retry_times >= #{maxRetryTimes} " +
            "AND create_time > DATE_SUB(NOW(), INTERVAL 1 DAY)")
    java.util.List<MessageConsumeLog> selectFailedMessages(@Param("maxRetryTimes") int maxRetryTimes);
}