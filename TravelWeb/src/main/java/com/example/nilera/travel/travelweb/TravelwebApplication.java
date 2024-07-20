package com.example.nilera.travel.travelweb;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan("com.example.nilera.travel.travelweb.dao")
public class TravelwebApplication {
    public static void main(String[] args) {
        SpringApplication.run(TravelwebApplication.class, args);
    }
}
