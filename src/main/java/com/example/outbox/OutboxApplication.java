package com.example.outbox;

import com.example.outbox.config.AppProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

@SpringBootApplication
@EnableConfigurationProperties(AppProperties.class)
public class OutboxApplication {

    public static void main(String[] args) {
        SpringApplication.run(OutboxApplication.class, args);
    }

    /** Single time source so every component agrees on "now" (and it can be faked in tests). */
    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }
}
