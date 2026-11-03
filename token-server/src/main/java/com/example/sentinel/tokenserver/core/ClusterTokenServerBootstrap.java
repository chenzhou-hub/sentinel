package com.example.sentinel.tokenserver.core;

import java.util.LinkedHashSet;
import java.util.Set;

import com.alibaba.csp.sentinel.cluster.flow.rule.ClusterFlowRuleManager;
import com.alibaba.csp.sentinel.cluster.flow.rule.ClusterParamFlowRuleManager;
import com.alibaba.csp.sentinel.cluster.server.ClusterTokenServer;
import com.alibaba.csp.sentinel.cluster.server.SentinelDefaultTokenServer;
import com.alibaba.csp.sentinel.cluster.server.ServerConstants;
import com.alibaba.csp.sentinel.cluster.server.config.ClusterServerConfigManager;
import com.alibaba.csp.sentinel.cluster.server.config.ServerFlowConfig;
import com.alibaba.csp.sentinel.cluster.server.config.ServerTransportConfig;
import com.example.sentinel.tokenserver.config.NacosProperties;
import com.example.sentinel.tokenserver.config.TokenServerProperties;
import com.example.sentinel.tokenserver.registry.NacosTokenServerRegistry;
import com.example.sentinel.tokenserver.rule.NacosRulePropertySupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * Token Server 启停编排。
 *
 * <p>启动顺序：规则数据源 → 通信/限流配置 → 命名空间集合 → 启动 Netty 服务 → 注册到 Nacos。
 * 注册必须放在服务真正监听之后，否则客户端可能拿到一个连不上的实例。</p>
 */
@Component
public class ClusterTokenServerBootstrap implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ClusterTokenServerBootstrap.class);

    private final TokenServerProperties properties;

    private final NacosProperties nacosProperties;

    private final NacosTokenServerRegistry registry;

    private final Object monitor = new Object();

    private volatile ClusterTokenServer tokenServer;

    private volatile boolean running;

    public ClusterTokenServerBootstrap(TokenServerProperties properties,
                                       NacosProperties nacosProperties,
                                       NacosTokenServerRegistry registry) {
        this.properties = properties;
        this.nacosProperties = nacosProperties;
        this.registry = registry;
    }

    @Override
    public void start() {
        synchronized (monitor) {
            if (running) {
                return;
            }
            TokenServerProperties.Server serverProperties = properties.getTokenServer();
            initRuleDataSource();
            initServerConfig(serverProperties);
            startTokenServer(serverProperties);
            registry.register();
            running = true;
        }
    }

    private void initRuleDataSource() {
        TokenServerProperties.Rule ruleProperties = properties.getRule();
        if (!ruleProperties.isEnabled()) {
            log.warn("[TokenServer] Nacos rule datasource is disabled, cluster rules will stay empty "
                    + "unless pushed by dashboard");
            return;
        }
        NacosRulePropertySupplier supplier = new NacosRulePropertySupplier(nacosProperties, ruleProperties);
        ClusterFlowRuleManager.setPropertySupplier(supplier::flowRuleProperty);
        ClusterParamFlowRuleManager.setPropertySupplier(supplier::paramFlowRuleProperty);
    }

    private void initServerConfig(TokenServerProperties.Server serverProperties) {
        // 必须在构造 SentinelDefaultTokenServer 之前加载，构造时会按该端口创建 Netty Transport Server
        ClusterServerConfigManager.loadGlobalTransportConfig(new ServerTransportConfig()
                .setPort(serverProperties.getPort())
                .setIdleSeconds(serverProperties.getIdleSeconds()));

        if (serverProperties.getMaxAllowedQps() != null) {
            ClusterServerConfigManager.loadGlobalFlowConfig(new ServerFlowConfig()
                    .setMaxAllowedQps(serverProperties.getMaxAllowedQps())
                    .setIntervalMs(serverProperties.getIntervalMs())
                    .setSampleCount(serverProperties.getSampleCount()));
        }

        // default 命名空间会被 Sentinel 自动追加，这里显式加上便于日志核对
        Set<String> namespaceSet = new LinkedHashSet<>(serverProperties.getNamespaceSet());
        namespaceSet.add(ServerConstants.DEFAULT_NAMESPACE);
        // 触发 applyNamespaceSetChange：为每个 namespace 注册规则属性并初始化全局 QPS 限流器
        ClusterServerConfigManager.loadServerNamespaceSet(namespaceSet);
        log.info("[TokenServer] Namespace set prepared: {}", ClusterServerConfigManager.getNamespaceSet());
    }

    private void startTokenServer(TokenServerProperties.Server serverProperties) {
        try {
            ClusterTokenServer server = new SentinelDefaultTokenServer();
            server.start();
            this.tokenServer = server;
            log.info("[TokenServer] Sentinel token server started, port={}, idleSeconds={}, maxAllowedQps={}",
                    ClusterServerConfigManager.getPort(),
                    ClusterServerConfigManager.getIdleSeconds(),
                    ClusterServerConfigManager.getMaxAllowedQps());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to start Sentinel token server on port " + serverProperties.getPort(), e);
        }
    }

    @Override
    public void stop() {
        synchronized (monitor) {
            if (!running) {
                return;
            }
            running = false;
            registry.deregister();
            ClusterTokenServer server = this.tokenServer;
            if (server != null) {
                try {
                    server.stop();
                    log.info("[TokenServer] Sentinel token server stopped");
                } catch (Exception e) {
                    log.warn("[TokenServer] Failed to stop Sentinel token server", e);
                }
                this.tokenServer = null;
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
