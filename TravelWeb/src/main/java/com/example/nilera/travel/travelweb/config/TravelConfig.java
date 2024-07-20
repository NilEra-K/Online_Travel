package com.example.nilera.travel.travelweb.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

// SpringBoot 中很多事情需要通过注解的方式来实现
// @... 代表注解
// @Configuration 注解表明这是一个 Spring Boot 的配置类
// @EnableWebMvc  开启 WebMvc 的功能, 这样 Spring Boot 就能自动处理 Web 相关的配置
@Configuration
@EnableWebMvc
public class TravelConfig implements WebMvcConfigurer {
    /**
     * @brief 设置服务器跨域访问策略: 所有 GET 和 POST 请求都可以跨域访问, 允许所有来源的请求, 即允许跨域请求
     *        因为我们的项目是前后端分离的, 即前端和后端不在一个服务器上, 所以需要处理跨域问题
     * @param registry
     */
    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")  // addMapping 方法的参数代表需要进行 CORS 映射的 HTTP方法及其路径, 这里是"/**", 意味着对所有HTTP方法进行映射
                .allowedOriginPatterns("*")     // allowedOriginPatterns 是一个正则表达式列表, 这里使用 "*" 表示任何来源都可以
                .allowedMethods("GET", "POST")  // allowedMethods是允许的 HTTP 方法，这里是 "GET,POST"
                .allowCredentials(true)         // 允许携带请求体的请求通过, 这里的 allowCredentials(true) 意味着在请求头中携带用户凭证信息是允许的
                .maxAge(3600);                  // maxAge 设置 CORS 跨域配置的缓存时间, 这里是 3600 秒
    }
}
