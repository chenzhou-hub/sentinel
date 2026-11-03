package com.example.sentinel.gateway.web;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.csp.sentinel.cluster.ClusterStateManager;
import com.alibaba.csp.sentinel.cluster.client.config.ClusterClientConfigManager;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import com.example.sentinel.gateway.cluster.TokenServerDiscovery;
import com.example.sentinel.gateway.config.GatewaySentinelProperties;
import com.example.sentinel.gateway.discovery.NacosServiceRegistrar;
import com.example.sentinel.gateway.rule.ClusterRuleMerger;
import com.example.sentinel.gateway.rule.FlowRuleSyncManager;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 网关限流巡检接口。
 */
@RestController
@RequestMapping("/gateway-sentinel")
public class GatewaySentinelInfoController {

    private final GatewaySentinelProperties properties;

    private final TokenServerDiscovery tokenServerDiscovery;

    private final FlowRuleSyncManager flowRuleSyncManager;

    private final NacosServiceRegistrar nacosServiceRegistrar;

    private final String appName;

    public GatewaySentinelInfoController(GatewaySentinelProperties properties,
                                         TokenServerDiscovery tokenServerDiscovery,
                                         FlowRuleSyncManager flowRuleSyncManager,
                                         NacosServiceRegistrar nacosServiceRegistrar,
                                         @Value("${spring.application.name}") String appName) {
        this.properties = properties;
        this.tokenServerDiscovery = tokenServerDiscovery;
        this.flowRuleSyncManager = flowRuleSyncManager;
        this.nacosServiceRegistrar = nacosServiceRegistrar;
        this.appName = appName;
    }

    @GetMapping("/info")
    public Map<String, Object> info() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("appName", appName);
        info.put("namespace", properties.getCluster().resolveNamespace(appName));
        info.put("dashboard", properties.getDashboard());
        info.put("apiPort", properties.getApiPort());

        Map<String, Object> nacos = new LinkedHashMap<>();
        nacos.put("registered", nacosServiceRegistrar.isRegistered());
        nacos.put("serviceName", nacosServiceRegistrar.getServiceName());
        nacos.put("group", nacosServiceRegistrar.getGroup());
        nacos.put("address", nacosServiceRegistrar.getRegisteredAddress());
        info.put("nacos", nacos);

        Map<String, Object> cluster = new LinkedHashMap<>();
        cluster.put("enabled", properties.getCluster().isEnabled());
        cluster.put("state", describeClusterState());
        cluster.put("clusterClientStarted", tokenServerDiscovery.isClusterClientStarted());
        cluster.put("tokenServerHost", tokenServerDiscovery.getTokenServerHost());
        cluster.put("tokenServerPort", tokenServerDiscovery.getTokenServerPort());
        cluster.put("matchedInstanceCount", tokenServerDiscovery.getMatchedInstanceCount());
        cluster.put("requestTimeoutMs", ClusterClientConfigManager.getRequestTimeout());
        long degradedSince = tokenServerDiscovery.getDegradedSince();
        cluster.put("degraded", tokenServerDiscovery.isDegraded());
        cluster.put("degradeCount", tokenServerDiscovery.getDegradeCount());
        cluster.put("degradedSince", degradedSince > 0 ? Instant.ofEpochMilli(degradedSince).toString() : null);
        cluster.put("degradedSeconds", degradedSince > 0
                ? (System.currentTimeMillis() - degradedSince) / 1000 : null);
        info.put("cluster", cluster);

        List<FlowRule> rules = FlowRuleManager.getRules();
        Map<String, Object> ruleInfo = new LinkedHashMap<>();
        ruleInfo.put("dataId", flowRuleSyncManager.getDataId());
        ruleInfo.put("group", flowRuleSyncManager.getGroup());
        ruleInfo.put("ruleCount", rules == null ? 0 : rules.size());
        ruleInfo.put("clusterRuleCount", ClusterRuleMerger.countClusterRules(rules));
        ruleInfo.put("autoClusterEnabled", properties.getRule().isAutoClusterEnabled());
        ruleInfo.put("publishEnabled", properties.getRule().isPublishEnabled());
        ruleInfo.put("lastSyncSource", flowRuleSyncManager.getLastSyncSource());
        ruleInfo.put("lastSyncTime", flowRuleSyncManager.getLastSyncTime());
        info.put("rule", ruleInfo);
        return info;
    }

    @GetMapping("/rules")
    public List<FlowRule> rules() {
        List<FlowRule> rules = FlowRuleManager.getRules();
        return rules == null ? List.of() : rules;
    }

    private String describeClusterState() {
        int mode = ClusterStateManager.getMode();
        return switch (mode) {
            case ClusterStateManager.CLUSTER_CLIENT -> "CLUSTER_CLIENT";
            case ClusterStateManager.CLUSTER_SERVER -> "CLUSTER_SERVER";
            default -> "CLUSTER_NOT_STARTED";
        };
    }
}
