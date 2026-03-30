package com.cassandratool.web;

import com.datastax.oss.driver.api.core.CqlSession;
import org.springframework.boot.web.servlet.ServletListenerRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.servlet.http.HttpSessionEvent;
import javax.servlet.http.HttpSessionListener;

@Configuration
public class WebConfig {

    @Bean
    public ServletListenerRegistrationBean<HttpSessionListener> cassandraSessionCleanup() {
        return new ServletListenerRegistrationBean<>(new HttpSessionListener() {
            @Override
            public void sessionDestroyed(HttpSessionEvent se) {
                Object o = se.getSession().getAttribute(WebCassandraSupport.ATTR_CQL_SESSION);
                if (o instanceof CqlSession) {
                    try {
                        ((CqlSession) o).close();
                    } catch (Exception ignored) {
                        // ignore
                    }
                }
            }
        });
    }
}
