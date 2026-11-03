package com.example.sentinel.gateway.discovery;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.example.sentinel.gateway.config.GatewaySentinelProperties;
import com.example.sentinel.gateway.config.NacosDiscoveryProperties;
import com.example.sentinel.gateway.config.NacosProperties;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.web.context.WebServerApplicationContext;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 把网关自身注册到 Nacos 服务列表。
 *
 * <p>本工程只依赖 nacos-client，没有引入 spring-cloud-alibaba 的注册中心实现，
 * 所以这里用 {@link NamingService} 手动注册/注销，服务名、分组、权重等由
 * {@code nacos.discovery.*} 控制。</p>
 *
 * <p>注册失败（例如 Nacos 启动晚于网关）不会阻塞启动，后台每 5 秒重试一次，
 * 进程退出时注销实例并关闭连接。</p>
 */
@Component
public class NacosServiceRegistrar implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(NacosServiceRegistrar.class);

    private static final long RETRY_INTERVAL_SECONDS = 5L;

    private final NacosProperties nacosProperties;

    private final NacosDiscoveryProperties discoveryProperties;

    private final GatewaySentinelProperties gatewaySentinelProperties;

    private final Environment environment;

    private final ApplicationContext applicationContext;

    private final String appName;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "nacos-service-registrar");
        thread.setDaemon(true);
        return thread;
    });

    private volatile NamingService namingService;

    private volatile Instance registeredInstance;

    private volatile int failureCount;

    public NacosServiceRegistrar(NacosProperties nacosProperties,
                                 NacosDiscoveryProperties discoveryProperties,
                                 GatewaySentinelProperties gatewaySentinelProperties,
                                 Environment environment,
                                 ApplicationContext applicationContext,
                                 @Value("${spring.application.name}") String appName) {
        this.nacosProperties = nacosProperties;
        this.discoveryProperties = discoveryProperties;
        this.gatewaySentinelProperties = gatewaySentinelProperties;
        this.environment = environment;
        this.applicationContext = applicationContext;
        this.appName = appName;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!discoveryProperties.isEnabled()) {
            log.info("[Gateway] Nacos registration is disabled (nacos.discovery.enabled=false)");
            return;
        }
        // 注册放在后台线程：Nacos 不可用时不影响网关启动与限流
        scheduler.scheduleWithFixedDelay(this::registerIfNeeded, 0, RETRY_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    private void registerIfNeeded() {
        if (registeredInstance != null) {
            return;
        }
        try {
            Instance instance = buildInstance();
            namingService().registerInstance(discoveryProperties.getServiceName(),
                    discoveryProperties.getGroup(), instance);
            this.registeredInstance = instance;
            this.failureCount = 0;
            log.info("[Gateway] Registered to Nacos, service={}, group={}, namespace={}, address={}:{}, ephemeral={}",
                    discoveryProperties.getServiceName(), discoveryProperties.getGroup(),
                    StringUtils.hasText(nacosProperties.getNamespace()) ? nacosProperties.getNamespace() : "public",
                    instance.getIp(), instance.getPort(), instance.isEphemeral());
        } catch (Exception e) {
            failureCount++;
            // 首次失败与之后每分钟提示一次，避免 Nacos 长时间不可用时刷日志
            if (failureCount == 1 || failureCount % 12 == 0) {
                log.warn("[Gateway] Failed to register gateway to Nacos (attempt {}), retry in {}s: {}",
                        failureCount, RETRY_INTERVAL_SECONDS, e.getMessage());
            } else {
                log.debug("[Gateway] Failed to register gateway to Nacos (attempt {})", failureCount, e);
            }
        }
    }

    private Instance buildInstance() {
        Instance instance = new Instance();
        instance.setServiceName(discoveryProperties.getServiceName());
        instance.setIp(resolveIp());
        instance.setPort(resolvePort());
        instance.setWeight(discoveryProperties.getWeight());
        instance.setEphemeral(discoveryProperties.isEphemeral());
        instance.setHealthy(true);
        instance.setEnabled(true);
        instance.setClusterName(discoveryProperties.getClusterName());
        instance.setMetadata(buildMetadata());
        return instance;
    }

    private Map<String, String> buildMetadata() {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("app", appName);
        metadata.put("component", "spring-cloud-gateway");
        metadata.put("sentinel.namespace", gatewaySentinelProperties.getCluster().resolveNamespace(appName));
        metadata.put("sentinel.apiPort", String.valueOf(gatewaySentinelProperties.getApiPort()));
        metadata.put("sentinel.dashboard", String.valueOf(gatewaySentinelProperties.getDashboard()));
        metadata.put("register.source", "sentinel-gateway");
        if (discoveryProperties.getMetadata() != null) {
            metadata.putAll(discoveryProperties.getMetadata());
        }
        return metadata;
    }

    private int resolvePort() {
        if (applicationContext instanceof WebServerApplicationContext webContext
                && webContext.getWebServer() != null) {
            int port = webContext.getWebServer().getPort();
            if (port > 0) {
                return port;
            }
        }
        return environment.getProperty("server.port", Integer.class, 8080);
    }

    private String resolveIp() {
        if (StringUtils.hasText(discoveryProperties.getIp())) {
            return discoveryProperties.getIp();
        }
        try {
            for (NetworkInterface networkInterface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
                if (!networkInterface.isUp() || networkInterface.isLoopback() || networkInterface.isVirtual()) {
                    continue;
                }
                for (InetAddress address : Collections.list(networkInterface.getInetAddresses())) {
                    if (address instanceof Inet4Address && address.isSiteLocalAddress()) {
                        return address.getHostAddress();
                    }
                }
            }
        } catch (Exception e) {
            log.debug("[Gateway] Failed to detect local IP from network interfaces", e);
        }
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            log.warn("[Gateway] Failed to detect local IP, fallback to 127.0.0.1", e);
            return "127.0.0.1";
        }
    }

    private NamingService namingService() throws Exception {
        NamingService service = this.namingService;
        if (service == null) {
            synchronized (this) {
                service = this.namingService;
                if (service == null) {
                    service = NacosFactory.createNamingService(nacosProperties.toProperties());
                    this.namingService = service;
                }
            }
        }
        return service;
    }

    @PreDestroy
    public void destroy() {
        scheduler.shutdownNow();
        NamingService naming = this.namingService;
        Instance instance = this.registeredInstance;
        if (naming != null && instance != null) {
            try {
                naming.deregisterInstance(discoveryProperties.getServiceName(),
                        discoveryProperties.getGroup(), instance);
                log.info("[Gateway] Deregistered from Nacos, service={}, address={}:{}",
                        discoveryProperties.getServiceName(), instance.getIp(), instance.getPort());
            } catch (Exception e) {
                log.warn("[Gateway] Failed to deregister gateway from Nacos", e);
            }
        }
        this.registeredInstance = null;
        if (naming != null) {
            try {
                naming.shutDown();
            } catch (Exception e) {
                log.debug("[Gateway] Failed to shutdown Nacos naming service", e);
            }
        }
    }

    public boolean isRegistered() {
        return registeredInstance != null;
    }

    public String getServiceName() {
        return discoveryProperties.getServiceName();
    }

    public String getGroup() {
        return discoveryProperties.getGroup();
    }

    public String getRegisteredAddress() {
        Instance instance = this.registeredInstance;
        return instance == null ? null : instance.getIp() + ":" + instance.getPort();
    }
}
