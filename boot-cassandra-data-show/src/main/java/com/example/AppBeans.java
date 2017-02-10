package com.example;

import com.datastax.driver.core.Session;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppBeans {

    @Bean
    public Session session() {
        return sessionManager().getSession();
    }

    @Bean
    public CassandraSessionManager sessionManager() {
        return new CassandraSessionManager();
    }
}
