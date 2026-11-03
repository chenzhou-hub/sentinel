package com.example.sentinel.gateway.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

/**
 * 网关 Sentinel 配置，对应 application.yml 中 sentinel.* 节点。
 */
@ConfigurationProperties(prefix = "sentinel")
public class GatewaySentinelProperties {

    /**
     * Sentinel Dashboard 地址（ip:port）。留空则不向控制台注册心跳。
     */
    private String dashboard;

    /**
     * Sentinel 命令端口（csp.sentinel.api.port），Dashboard 通过该端口下发规则。
     */
    private Integer apiPort;

    private final Cluster cluster = new Cluster();

    private final Rule rule = new Rule();

    public String getDashboard() {
        return dashboard;
    }

    public void setDashboard(String dashboard) {
        this.dashboard = dashboard;
    }

    public Integer getApiPort() {
        return apiPort;
    }

    public void setApiPort(Integer apiPort) {
        this.apiPort = apiPort;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public Rule getRule() {
        return rule;
    }

    /**
     * 集群流控（Token Client）配置。
     */
    public static class Cluster {

        /**
         * 是否作为集群客户端接入 token-server。关闭后只做单机限流。
         */
        private boolean enabled = true;

        /**
         * 集群命名空间，必须与 token-server 的 namespace-set 一致；默认取 spring.application.name。
         */
        private String namespace;

        /**
         * token-server 在 Nacos 中注册的服务名。
         */
        private String serviceName = "sentinel-token-server";

        /**
         * token-server 注册所在的分组。
         */
        private String group = "DEFAULT_GROUP";

        /**
         * 向 token-server 申请令牌的超时时间（毫秒），默认 20ms。
         */
        private int requestTimeout = 20;

        /**
         * 连接 token-server 的超时时间（毫秒），
         * 该值同时用于判断 token-server 是否已经离线。
         */
        private int connectTimeoutMillis = 3000;

        /**
         * 启动时找不到可用 token-server 是否直接启动失败。
         * 默认 false：退化为本地限流，后续发现到实例会自动切换。
         */
        private boolean failFast = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getNamespace() {
            return namespace;
        }

        public void setNamespace(String namespace) {
            this.namespace = namespace;
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

        public int getRequestTimeout() {
            return requestTimeout;
        }

        public void setRequestTimeout(int requestTimeout) {
            this.requestTimeout = requestTimeout;
        }

        public int getConnectTimeoutMillis() {
            return connectTimeoutMillis;
        }

        public void setConnectTimeoutMillis(int connectTimeoutMillis) {
            this.connectTimeoutMillis = connectTimeoutMillis;
        }

        public boolean isFailFast() {
            return failFast;
        }

        public void setFailFast(boolean failFast) {
            this.failFast = failFast;
        }

        /**
         * 解析实际使用的命名空间：显式配置优先，否则使用应用名。
         */
        public String resolveNamespace(String appName) {
            return StringUtils.hasText(namespace) ? namespace.trim() : appName;
        }
    }

    /**
     * 规则数据源与集群规则补全配置。
     */
    public static class Rule {

        /**
         * 是否启用 Nacos 规则数据源（与 token-server 共用同一份配置）。
         */
        private boolean enabled = true;

        /**
         * Nacos 配置分组，需与 token-server 的 sentinel.rule.group 一致。
         */
        private String group = "SENTINEL_GROUP";

        /**
         * 流控规则 dataId 模板，{namespace} 会替换为命名空间。
         */
        private String dataIdPattern = "{namespace}-flow-rules";

        /**
         * 是否允许把 Dashboard 下发（或本地合并后）的规则回写到 Nacos，
         * 从而让 token-server 同步到集群规则。
         */
        private boolean publishEnabled = true;

        /**
         * Dashboard 下发的普通规则是否自动补全 clusterConfig 变成集群规则。
         */
        private boolean autoClusterEnabled = true;

        /**
         * 自动补全时的阈值类型：0 单机均摊，1 全局阈值。
         */
        private int thresholdType = 0;

        /**
         * 自动补全时的集群限流策略：0 单机均摊，1 全局匀速。
         */
        private int clusterStrategy = 0;

        /**
         * token-server 不可用或请求失败时是否退化为本地限流。
         */
        private boolean fallbackToLocalWhenFail = true;

        /**
         * 集群统计窗口格子数。
         */
        private int sampleCount = 10;

        /**
         * 集群统计窗口时长（毫秒）。
         */
        private int windowIntervalMs = 1000;

        /**
         * 自动生成 flowId 的起始值（flowId 必须全局唯一且为正数）。
         */
        private long flowIdBase = 100000L;

        /**
         * 自动生成 flowId 的取值范围，最终 flowId = base + crc32(resource) % mod。
         */
        private long flowIdMod = 1_000_000L;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public String getDataIdPattern() {
            return dataIdPattern;
        }

        public void setDataIdPattern(String dataIdPattern) {
            this.dataIdPattern = dataIdPattern;
        }

        public boolean isPublishEnabled() {
            return publishEnabled;
        }

        public void setPublishEnabled(boolean publishEnabled) {
            this.publishEnabled = publishEnabled;
        }

        public boolean isAutoClusterEnabled() {
            return autoClusterEnabled;
        }

        public void setAutoClusterEnabled(boolean autoClusterEnabled) {
            this.autoClusterEnabled = autoClusterEnabled;
        }

        public int getThresholdType() {
            return thresholdType;
        }

        public void setThresholdType(int thresholdType) {
            this.thresholdType = thresholdType;
        }

        public int getClusterStrategy() {
            return clusterStrategy;
        }

        public void setClusterStrategy(int clusterStrategy) {
            this.clusterStrategy = clusterStrategy;
        }

        public boolean isFallbackToLocalWhenFail() {
            return fallbackToLocalWhenFail;
        }

        public void setFallbackToLocalWhenFail(boolean fallbackToLocalWhenFail) {
            this.fallbackToLocalWhenFail = fallbackToLocalWhenFail;
        }

        public int getSampleCount() {
            return sampleCount;
        }

        public void setSampleCount(int sampleCount) {
            this.sampleCount = sampleCount;
        }

        public int getWindowIntervalMs() {
            return windowIntervalMs;
        }

        public void setWindowIntervalMs(int windowIntervalMs) {
            this.windowIntervalMs = windowIntervalMs;
        }

        public long getFlowIdBase() {
            return flowIdBase;
        }

        public void setFlowIdBase(long flowIdBase) {
            this.flowIdBase = flowIdBase;
        }

        public long getFlowIdMod() {
            return flowIdMod;
        }

        public void setFlowIdMod(long flowIdMod) {
            this.flowIdMod = flowIdMod;
        }
    }
}
