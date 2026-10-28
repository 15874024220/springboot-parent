package com.example.rocketmq.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;

/**
 * Web MVC配置
 * 配置消息转换器、拦截器等
 *
 * @author example
 * 
 */
@Slf4j
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * 配置HTTP消息转换器
     * 使用自定义的Jackson配置
     */
    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
        log.info("配置HTTP消息转换器");

        // 创建Jackson消息转换器
        MappingJackson2HttpMessageConverter jacksonConverter = new MappingJackson2HttpMessageConverter();
        jacksonConverter.setObjectMapper(objectMapper);

        // 将自定义转换器放在第一位
        converters.add(0, jacksonConverter);

        log.info("HTTP消息转换器配置完成");
    }

    // 如果需要配置拦截器，可以重写addInterceptors方法
    // @Override
    // public void addInterceptors(InterceptorRegistry registry) {
    //     // 添加日志拦截器、权限拦截器等
    //     registry.addInterceptor(new LogInterceptor()).addPathPatterns("/**");
    // }
}