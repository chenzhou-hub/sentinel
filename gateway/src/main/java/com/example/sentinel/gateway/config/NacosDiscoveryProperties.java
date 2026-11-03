package com.example.sentinel.gateway.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 网关自身注册到 Nacos 的配置，对应 application.yml 中 nacos.discovery.* 节点。
 *
 * <p>注意与 {@code sentinel.cluster.namespace} 区分：后者是 Sentinel 集群限流的命名空间，
 * 这里是网关在 Nacos 服务列表里的服务名。</p>
 */
@ConfigurationProperties(prefix = "nacos.discovery")
public class NacosDiscoveryProperties {

    /**
     * 是否把网关注册到 Nacos 服务列表。
     */
    private boolean enabled = true;

    /**
     * 注册到 Nacos 的服务名。
     */
    private String serviceName = "sentinel-gateway";

    /**
     * 服务分组。
     */
    private String group = "DEFAULT_GROUP";

    /**
     * 集群名，仅用于 Nacos 控制台分组展示。
     */
    private String clusterName = "DEFAULT";

    /**
     * 注册 IP，留空则自动挑选本机内网 IPv4。
     */
    private String ip;

    /**
     * 权重，配合 Nacos 的权重负载使用。
     */
    private double weight = 1.0D;

    /**
     * 是否临时实例（临时实例由客户端心跳维持，进程退出自动剔除）。
     */
    private boolean ephemeral = true;

    /**
     * 附加元数据，会覆盖框架默认写入的同名 key。
     */
    private Map<String, String> metadata = new LinkedHashMap<>();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public boolean isEphemeral() {
        return ephemeral;
    }

    public void setEphemeral(boolean ephemeral) {
        this.ephemeral = ephemeral;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }
}
