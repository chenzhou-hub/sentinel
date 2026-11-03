package com.example.sentinel.gateway.config;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.util.StringUtils;

/**
 * Sentinel 的 Dashboard 地址、应用名、命令端口都通过 JVM 系统属性读取，
 * 这里在 Spring 环境准备阶段把 yml 配置桥接为系统属性。
 *
 * <p>应用名会作为集群流控的 namespace，必须与 token-server 的 namespace-set 保持一致。</p>
 */
public class SentinelEnvironmentPostProcessor implements EnvironmentPostProcessor, Ordered {

    private static final String DASHBOARD_SERVER_KEY = "csp.sentinel.dashboard.server";

    private static final String PROJECT_NAME_KEY = "project.name";

    private static final String API_PORT_KEY = "csp.sentinel.api.port";

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {
        String dashboard = environment.getProperty("sentinel.dashboard");
        if (StringUtils.hasText(dashboard) && System.getProperty(DASHBOARD_SERVER_KEY) == null) {
            System.setProperty(DASHBOARD_SERVER_KEY, dashboard.trim());
        }
        String appName = environment.getProperty("spring.application.name");
        if (StringUtils.hasText(appName) && System.getProperty(PROJECT_NAME_KEY) == null) {
            System.setProperty(PROJECT_NAME_KEY, appName.trim());
        }
        String apiPort = environment.getProperty("sentinel.api-port");
        if (StringUtils.hasText(apiPort) && System.getProperty(API_PORT_KEY) == null) {
            System.setProperty(API_PORT_KEY, apiPort.trim());
        }
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }
}
