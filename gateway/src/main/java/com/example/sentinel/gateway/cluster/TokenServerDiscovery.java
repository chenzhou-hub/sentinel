package com.example.sentinel.gateway.cluster;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.alibaba.csp.sentinel.cluster.ClusterStateManager;
import com.alibaba.csp.sentinel.cluster.client.config.ClusterClientAssignConfig;
import com.alibaba.csp.sentinel.cluster.client.config.ClusterClientConfig;
import com.alibaba.csp.sentinel.cluster.client.config.ClusterClientConfigManager;
import com.alibaba.csp.sentinel.property.DynamicSentinelProperty;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.example.sentinel.gateway.config.GatewaySentinelProperties;
import com.example.sentinel.gateway.config.NacosProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

/**
 * 从 Nacos 发现 token-server，并把地址动态注册到 Sentinel 集群客户端。
 *
 * <p>token-server 注册实例时会带上 metadata.namespaces（逗号分隔），
 * 这里按本应用的集群命名空间过滤，保证不会连到承载其他命名空间的实例上。</p>
 */
@Component
public class TokenServerDiscovery implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(TokenServerDiscovery.class);

    private final GatewaySentinelProperties properties;

    private final NacosProperties nacosProperties;

    private final String appName;

    private final Object monitor = new Object();

    private final DynamicSentinelProperty<ClusterClientAssignConfig> assignProperty =
            new DynamicSentinelProperty<>();

    private volatile boolean running;

    private volatile NamingService namingService;

    private volatile EventListener namingListener;

    private volatile String tokenServerHost;

    private volatile Integer tokenServerPort;

    private volatile int matchedInstanceCount;

    private volatile boolean clusterClientStarted;

    /**
     * 当前是否处于"降级为本地限流"状态（没有可用的 token-server）。
     */
    private volatile boolean degraded;

    /**
     * 最近一次进入降级的时间戳（毫秒），0 表示未降级。
     */
    private volatile long degradedSince;

    /**
     * 累计降级次数，用于告警/巡检。
     */
    private volatile long degradeCount;

    public TokenServerDiscovery(GatewaySentinelProperties properties,
                                NacosProperties nacosProperties,
                                @Value("${spring.application.name}") String appName) {
        this.properties = properties;
        this.nacosProperties = nacosProperties;
        this.appName = appName;
    }

    @Override
    public void start() {
        synchronized (monitor) {
            if (running) {
                return;
            }
            GatewaySentinelProperties.Cluster cluster = properties.getCluster();
            if (!cluster.isEnabled()) {
                log.warn("[Gateway] Cluster flow is disabled, requests will be limited locally only");
                ClusterStateManager.applyState(ClusterStateManager.CLUSTER_NOT_STARTED);
                running = true;
                return;
            }
            // 客户端配置必须在开启集群状态前注册，否则首次连接拿不到超时配置
            ClusterClientConfigManager.registerClientConfigProperty(new DynamicSentinelProperty<>(
                    new ClusterClientConfig().setRequestTimeout(cluster.getRequestTimeout())));
            ClusterClientConfigManager.registerServerAssignProperty(assignProperty);
            try {
                NamingService naming = NacosFactory.createNamingService(nacosProperties.toProperties());
                this.namingService = naming;
                EventListener listener = event -> {
                    if (event instanceof NamingEvent namingEvent) {
                        refresh(namingEvent.getInstances());
                    }
                };
                this.namingListener = listener;
                naming.subscribe(cluster.getServiceName(), cluster.getGroup(), listener);
                refresh(naming.selectInstances(cluster.getServiceName(), cluster.getGroup(), true));
            } catch (Exception e) {
                handleDiscoveryFailure(e);
            }
            running = true;
        }
    }

    private void refresh(List<Instance> instances) {
        String namespace = properties.getCluster().resolveNamespace(appName);
        List<Instance> matched = selectInstances(instances, namespace);
        matchedInstanceCount = matched.size();
        if (matched.isEmpty()) {
            downgradeToLocal(namespace);
            return;
        }
        Instance target = matched.get(0);
        applyTarget(target.getIp(), target.getPort(), namespace);
    }

    /**
     * 从 Nacos 返回的实例中挑选本应用可用的 token-server。
     *
     * <p>排序规则：权重倒序 → IP 升序 → 端口升序。后两级保证同一 namespace 下的多个客户端
     * 在权重相同时选出**同一个**实例，避免客户端分散到不同 token-server 导致全局阈值被拆成多份。</p>
     */
    static List<Instance> selectInstances(List<Instance> instances, String namespace) {
        return (instances == null ? List.<Instance>of() : instances).stream()
                .filter(Instance::isHealthy)
                .filter(instance -> supportsNamespace(instance, namespace))
                .sorted(Comparator.comparingDouble(Instance::getWeight).reversed()
                        .thenComparing(instance -> instance.getIp() == null ? "" : instance.getIp())
                        .thenComparingInt(Instance::getPort))
                .toList();
    }

    static boolean supportsNamespace(Instance instance, String namespace) {
        String namespaces = instance.getMetadata() == null ? null : instance.getMetadata().get("namespaces");
        if (namespaces == null || namespaces.isBlank()) {
            return false;
        }
        return Arrays.stream(namespaces.split(","))
                .map(String::trim)
                .anyMatch(namespace::equals);
    }

    private void applyTarget(String host, int port, String namespace) {
        boolean changed = !Objects.equals(host, tokenServerHost) || !Objects.equals(port, tokenServerPort);
        tokenServerHost = host;
        tokenServerPort = port;
        if (changed) {
            assignProperty.updateValue(new ClusterClientAssignConfig(host, port));
            log.info("[Gateway] Token server assigned, host={}, port={}, namespace={}", host, port, namespace);
        }
        if (!clusterClientStarted) {
            ClusterStateManager.applyState(ClusterStateManager.CLUSTER_CLIENT);
            clusterClientStarted = true;
            log.info("[Gateway] Sentinel cluster client enabled, namespace={}, candidates={}",
                    namespace, matchedInstanceCount);
        }
        if (degraded) {
            long duration = degradedSince > 0 ? System.currentTimeMillis() - degradedSince : 0L;
            degraded = false;
            degradedSince = 0L;
            log.info("[Gateway] Token server available again, recovered from local limiting after {}ms", duration);
        }
    }

    /**
     * 没有可用 token-server 时主动切回本地限流。
     *
     * <p>不做这一步的话，客户端会一直向失效地址发令牌请求，每个请求都要等满
     * {@code sentinel.cluster.request-timeout} 才 fallback 到本地，
     * 白白增加延迟、压低吞吐。</p>
     */
    private void downgradeToLocal(String namespace) {
        tokenServerHost = null;
        tokenServerPort = null;
        if (clusterClientStarted) {
            ClusterStateManager.applyState(ClusterStateManager.CLUSTER_NOT_STARTED);
            clusterClientStarted = false;
            log.warn("[Gateway] No healthy token-server for namespace {} in Nacos, "
                    + "sentinel cluster client stopped and degraded to local limiting", namespace);
        }
        if (!degraded) {
            degraded = true;
            degradedSince = System.currentTimeMillis();
            degradeCount++;
            log.warn("[Gateway] Degraded to local limiting, namespace={}, candidates={}",
                    namespace, matchedInstanceCount);
        }
    }

    private void handleDiscoveryFailure(Exception e) {
        if (properties.getCluster().isFailFast()) {
            throw new IllegalStateException("Failed to discover token-server from Nacos: "
                    + nacosProperties.getServerAddr(), e);
        }
        log.error("[Gateway] Failed to discover token-server from Nacos {}, cluster flow falls back to local limiting",
                nacosProperties.getServerAddr(), e);
    }

    @Override
    public void stop() {
        synchronized (monitor) {
            if (!running) {
                return;
            }
            running = false;
            GatewaySentinelProperties.Cluster cluster = properties.getCluster();
            NamingService naming = this.namingService;
            EventListener listener = this.namingListener;
            if (naming != null && listener != null) {
                try {
                    naming.unsubscribe(cluster.getServiceName(), cluster.getGroup(), listener);
                } catch (Exception e) {
                    log.warn("[Gateway] Failed to unsubscribe token-server from Nacos", e);
                }
                this.namingListener = null;
            }
            if (naming != null) {
                try {
                    naming.shutDown();
                } catch (Exception e) {
                    log.warn("[Gateway] Failed to shutdown Nacos naming service", e);
                }
                this.namingService = null;
            }
            if (clusterClientStarted) {
                ClusterStateManager.applyState(ClusterStateManager.CLUSTER_NOT_STARTED);
                clusterClientStarted = false;
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /**
     * 早于 WebServerStartStopLifecycle（Integer.MAX_VALUE - 1）启动，避免服务已接流量但集群状态未就绪。
     */
    @Override
    public int getPhase() {
        return 0;
    }

    public String getTokenServerHost() {
        return tokenServerHost;
    }

    public Integer getTokenServerPort() {
        return tokenServerPort;
    }

    public int getMatchedInstanceCount() {
        return matchedInstanceCount;
    }

    public boolean isClusterClientStarted() {
        return clusterClientStarted;
    }

    /**
     * 当前是否已降级为本地限流（没有可用 token-server）。
     */
    public boolean isDegraded() {
        return degraded;
    }

    /**
     * 最近一次进入降级的时间戳（毫秒），未降级时为 0。
     */
    public long getDegradedSince() {
        return degradedSince;
    }

    /**
     * 累计降级次数。
     */
    public long getDegradeCount() {
        return degradeCount;
    }
}
