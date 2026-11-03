package com.example.sentinel.tokenserver.config;

import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Token Server 配置，对应 application.yml 中 sentinel.* 节点。
 */
@ConfigurationProperties(prefix = "sentinel")
public class TokenServerProperties {

    /**
     * Sentinel Dashboard 地址（ip:port）。为空时 Token Server 不向控制台注册心跳，
     * 仅作为纯 Token Server 运行。
     */
    private String dashboard;

    private final Server tokenServer = new Server();

    private final Rule rule = new Rule();

    public String getDashboard() {
        return dashboard;
    }

    public void setDashboard(String dashboard) {
        this.dashboard = dashboard;
    }

    public Server getTokenServer() {
        return tokenServer;
    }

    public Rule getRule() {
        return rule;
    }

    /**
     * Token Server 本体配置。
     */
    public static class Server {

        /**
         * Token Server 监听端口，Token Client 直连该端口（TCP/Netty）。
         */
        private int port = 18730;

        /**
         * 连接最大空闲时间（秒）。
         */
        private int idleSeconds = 600;

        /**
         * 命名空间集合，通常填 Token Client 所在应用的 spring.application.name。
         * 启动时会自动追加 Sentinel 内置的 default 命名空间。
         */
        private Set<String> namespaceSet = new LinkedHashSet<>();

        /**
         * Token Server 全局最大 QPS 保护，null 表示不限制。生产环境建议显式设置。
         */
        private Double maxAllowedQps;

        /**
         * 统计窗口时长（毫秒），仅在设置 maxAllowedQps 时生效。
         */
        private int intervalMs = 1000;

        /**
         * 统计窗口格子数，intervalMs % sampleCount 必须为 0。
         */
        private int sampleCount = 10;

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public int getIdleSeconds() {
            return idleSeconds;
        }

        public void setIdleSeconds(int idleSeconds) {
            this.idleSeconds = idleSeconds;
        }

        public Set<String> getNamespaceSet() {
            return namespaceSet;
        }

        public void setNamespaceSet(Set<String> namespaceSet) {
            this.namespaceSet = namespaceSet;
        }

        public Double getMaxAllowedQps() {
            return maxAllowedQps;
        }

        public void setMaxAllowedQps(Double maxAllowedQps) {
            this.maxAllowedQps = maxAllowedQps;
        }

        public int getIntervalMs() {
            return intervalMs;
        }

        public void setIntervalMs(int intervalMs) {
            this.intervalMs = intervalMs;
        }

        public int getSampleCount() {
            return sampleCount;
        }

        public void setSampleCount(int sampleCount) {
            this.sampleCount = sampleCount;
        }
    }

    /**
     * 集群规则（Nacos 配置中心）相关配置。
     */
    public static class Rule {

        /**
         * 是否启用 Nacos 规则数据源。关闭后只能通过 Dashboard 的
         * /cluster/server/modifyRules 等命令手工下发规则。
         */
        private boolean enabled = true;

        /**
         * Nacos 配置分组。
         */
        private String group = "SENTINEL_GROUP";

        /**
         * 集群流控规则 dataId 模板，{namespace} 会被替换为命名空间。
         */
        private String dataIdPattern = "{namespace}-flow-rules";

        /**
         * 集群热点参数规则 dataId 模板。
         */
        private String paramDataIdPattern = "{namespace}-param-flow-rules";

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

        public String getParamDataIdPattern() {
            return paramDataIdPattern;
        }

        public void setParamDataIdPattern(String paramDataIdPattern) {
            this.paramDataIdPattern = paramDataIdPattern;
        }
    }
}
