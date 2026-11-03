package com.example.sentinel.tokenserver.registry;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.alibaba.csp.sentinel.util.HostNameUtil;
import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.example.sentinel.tokenserver.config.NacosProperties;
import com.example.sentinel.tokenserver.config.TokenServerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 把 Token Server 注册到 Nacos，供 Token Client 做服务发现。
 *
 * <p>注册的是临时实例（ephemeral），nacos-client 会自动发送心跳，进程退出后实例自动摘除。</p>
 */
@Component
public class NacosTokenServerRegistry {

    private static final Logger log = LoggerFactory.getLogger(NacosTokenServerRegistry.class);

    private final NacosProperties nacosProperties;

    private final TokenServerProperties tokenServerProperties;

    private volatile NamingService namingService;

    private volatile Instance registeredInstance;

    private volatile String serviceName;

    private volatile String group;

    public NacosTokenServerRegistry(NacosProperties nacosProperties,
                                    TokenServerProperties tokenServerProperties) {
        this.nacosProperties = nacosProperties;
        this.tokenServerProperties = tokenServerProperties;
    }

    /**
     * 注册到 Nacos，注册失败直接抛出异常，避免出现"服务已启动但客户端发现不到"的中间状态。
     */
    public void register() {
        NacosProperties.Discovery discovery = nacosProperties.getDiscovery();
        if (!discovery.isEnabled()) {
            log.warn("[TokenServer] Nacos discovery is disabled, Token Client must be configured manually");
            return;
        }
        this.serviceName = discovery.getServiceName();
        this.group = discovery.getGroup();
        Instance instance = buildInstance(discovery);
        try {
            NamingService naming = NacosFactory.createNamingService(nacosProperties.toProperties());
            naming.registerInstance(serviceName, group, instance);
            this.namingService = naming;
            this.registeredInstance = instance;
            log.info("[TokenServer] Registered to Nacos successfully, serverAddr={}, serviceName={}, group={}, "
                            + "instance={}:{}, metadata={}",
                    nacosProperties.getServerAddr(), serviceName, group, instance.getIp(), instance.getPort(),
                    instance.getMetadata());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to register token server to Nacos: " + nacosProperties.getServerAddr(), e);
        }
    }

    public void deregister() {
        NamingService naming = this.namingService;
        Instance instance = this.registeredInstance;
        if (naming == null || instance == null) {
            return;
        }
        try {
            naming.deregisterInstance(serviceName, group, instance);
            log.info("[TokenServer] Deregistered from Nacos, serviceName={}, instance={}:{}",
                    serviceName, instance.getIp(), instance.getPort());
        } catch (Exception e) {
            log.warn("[TokenServer] Failed to deregister from Nacos", e);
        } finally {
            try {
                naming.shutDown();
            } catch (Exception ignored) {
                // ignore
            }
            this.namingService = null;
            this.registeredInstance = null;
        }
    }

    /**
     * 是否已成功注册到 Nacos。
     */
    public boolean isRegistered() {
        return registeredInstance != null;
    }

    public String getServiceName() {
        return serviceName;
    }

    public Instance getRegisteredInstance() {
        return registeredInstance;
    }

    private Instance buildInstance(NacosProperties.Discovery discovery) {
        Set<String> namespaceSet = tokenServerProperties.getTokenServer().getNamespaceSet();
        Instance instance = new Instance();
        instance.setIp(resolveIp(discovery));
        instance.setPort(tokenServerProperties.getTokenServer().getPort());
        instance.setWeight(discovery.getWeight());
        instance.setEphemeral(true);
        instance.setHealthy(true);
        instance.setEnabled(true);
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("namespaces", String.join(",", namespaceSet));
        metadata.put("idleSeconds", String.valueOf(tokenServerProperties.getTokenServer().getIdleSeconds()));
        instance.setMetadata(metadata);
        return instance;
    }

    /**
     * 解析注册 IP：优先使用显式配置，其次挑选本机内网 IPv4（跳过回环与虚拟网卡），
     * 最后退回 Sentinel 的 hostname 工具。
     *
     * <p>不跳过虚拟网卡时会注册成 WSL/Hyper-V 的网段地址（如 172.19.0.1），
     * Token Client 从别的网段连不上。</p>
     */
    private String resolveIp(NacosProperties.Discovery discovery) {
        if (StringUtils.hasText(discovery.getIp())) {
            return discovery.getIp();
        }
        String detected = detectSiteLocalIpv4();
        return StringUtils.hasText(detected) ? detected : HostNameUtil.getIp();
    }

    private String detectSiteLocalIpv4() {
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
            log.warn("[TokenServer] Failed to detect local IP from network interfaces, fallback to hostname util", e);
        }
        return null;
    }
}
