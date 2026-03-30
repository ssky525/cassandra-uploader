package com.cassandratool.web;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
public class AsyncConfig {

    public static final String LOAD_EXECUTOR = "loadExecutor";

    @Bean(name = LOAD_EXECUTOR)
    public Executor loadExecutor() {
        ThreadPoolTaskExecutor ex = new ThreadPoolTaskExecutor();
        ex.setCorePoolSize(2);
        ex.setMaxPoolSize(6);
        ex.setQueueCapacity(50);
        ex.setThreadNamePrefix("csv-load-");
        ex.initialize();
        return ex;
    }
}
