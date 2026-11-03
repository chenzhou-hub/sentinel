package com.example.sentinel.gateway.rule;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.zip.CRC32;

import com.alibaba.csp.sentinel.slots.block.flow.ClusterFlowConfig;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.example.sentinel.gateway.config.GatewaySentinelProperties;
import org.springframework.stereotype.Component;

/**
 * 把规则补全成"集群规则"。
 *
 * <p>Sentinel Dashboard 的流控规则编辑框不支持集群模式，规则下发到网关时通常没有
 * clusterMode / clusterConfig，token-server 会直接忽略这类规则。这里在规则生效前统一补全：</p>
 * <ul>
 *     <li>已有 clusterConfig 的规则保持不变（来自 Nacos 或手工配置）；</li>
 *     <li>没有 clusterConfig 的规则复用同一 resource 上一次的 flowId，保证阈值连续；</li>
 *     <li>全新的 resource 用 crc32(resource) 生成稳定的 flowId，避免重启后变化。</li>
 * </ul>
 */
@Component
public class ClusterRuleMerger {

    private final GatewaySentinelProperties properties;

    public ClusterRuleMerger(GatewaySentinelProperties properties) {
        this.properties = properties;
    }

    public List<FlowRule> merge(List<FlowRule> incoming, List<FlowRule> previousEffectiveRules) {
        List<FlowRule> source = incoming == null ? List.of() : incoming;
        List<FlowRule> result = new ArrayList<>(source.size());
        for (FlowRule rule : source) {
            result.add(copy(rule));
        }
        GatewaySentinelProperties.Rule ruleProperties = properties.getRule();
        if (!properties.getCluster().isEnabled() || !ruleProperties.isAutoClusterEnabled()) {
            return result;
        }

        Map<String, ClusterFlowConfig> previousConfigs = indexPreviousConfigs(previousEffectiveRules);
        Set<Long> usedFlowIds = collectFlowIds(result);
        for (FlowRule rule : result) {
            ClusterFlowConfig existing = rule.getClusterConfig();
            if (existing != null && existing.getFlowId() != null && existing.getFlowId() > 0) {
                // 已经是集群规则，只补齐统计窗口等默认值
                fillMissingDefaults(existing, ruleProperties);
                rule.setClusterMode(true);
                continue;
            }
            ClusterFlowConfig config = buildConfig(rule.getResource(), previousConfigs.get(rule.getResource()),
                    usedFlowIds, ruleProperties);
            usedFlowIds.add(config.getFlowId());
            rule.setClusterMode(true);
            rule.setClusterConfig(config);
        }
        return result;
    }

    private Map<String, ClusterFlowConfig> indexPreviousConfigs(List<FlowRule> previousRules) {
        Map<String, ClusterFlowConfig> configs = new HashMap<>();
        if (previousRules == null) {
            return configs;
        }
        for (FlowRule rule : previousRules) {
            ClusterFlowConfig config = rule.getClusterConfig();
            if (rule.getResource() != null && config != null && config.getFlowId() != null
                    && config.getFlowId() > 0) {
                configs.putIfAbsent(rule.getResource(), config);
            }
        }
        return configs;
    }

    private Set<Long> collectFlowIds(List<FlowRule> rules) {
        Set<Long> flowIds = new HashSet<>();
        for (FlowRule rule : rules) {
            ClusterFlowConfig config = rule.getClusterConfig();
            if (config != null && config.getFlowId() != null && config.getFlowId() > 0) {
                flowIds.add(config.getFlowId());
            }
        }
        return flowIds;
    }

    private ClusterFlowConfig buildConfig(String resource,
                                          ClusterFlowConfig previous,
                                          Set<Long> usedFlowIds,
                                          GatewaySentinelProperties.Rule ruleProperties) {
        if (previous != null) {
            ClusterFlowConfig config = copy(previous);
            if (config.getFlowId() == null || usedFlowIds.contains(config.getFlowId())) {
                config.setFlowId(nextFlowId(resource, usedFlowIds, ruleProperties));
            }
            fillMissingDefaults(config, ruleProperties);
            return config;
        }
        ClusterFlowConfig config = new ClusterFlowConfig()
                .setThresholdType(ruleProperties.getThresholdType())
                .setStrategy(ruleProperties.getClusterStrategy())
                .setFallbackToLocalWhenFail(ruleProperties.isFallbackToLocalWhenFail())
                .setSampleCount(ruleProperties.getSampleCount())
                .setWindowIntervalMs(ruleProperties.getWindowIntervalMs());
        config.setFlowId(nextFlowId(resource, usedFlowIds, ruleProperties));
        return config;
    }

    private void fillMissingDefaults(ClusterFlowConfig config, GatewaySentinelProperties.Rule ruleProperties) {
        if (config.getSampleCount() <= 0) {
            config.setSampleCount(ruleProperties.getSampleCount());
        }
        if (config.getWindowIntervalMs() <= 0) {
            config.setWindowIntervalMs(ruleProperties.getWindowIntervalMs());
        }
    }

    private long nextFlowId(String resource, Set<Long> usedFlowIds, GatewaySentinelProperties.Rule ruleProperties) {
        long base = Math.max(1L, ruleProperties.getFlowIdBase());
        long mod = Math.max(1L, ruleProperties.getFlowIdMod());
        long candidate = base + crc32(resource) % mod;
        int attempts = 0;
        while (usedFlowIds.contains(candidate) && attempts++ < mod) {
            candidate = base + (candidate - base + 1) % mod;
        }
        return candidate;
    }

    private long crc32(String resource) {
        CRC32 crc32 = new CRC32();
        crc32.update(String.valueOf(resource).getBytes(StandardCharsets.UTF_8));
        return crc32.getValue() & 0x7FFFFFFFL;
    }

    private FlowRule copy(FlowRule source) {
        FlowRule target = new FlowRule();
        target.setResource(source.getResource());
        target.setLimitApp(source.getLimitApp());
        target.setGrade(source.getGrade());
        target.setCount(source.getCount());
        target.setStrategy(source.getStrategy());
        target.setRefResource(source.getRefResource());
        target.setControlBehavior(source.getControlBehavior());
        target.setWarmUpPeriodSec(source.getWarmUpPeriodSec());
        target.setMaxQueueingTimeMs(source.getMaxQueueingTimeMs());
        target.setClusterMode(source.isClusterMode());
        if (source.getClusterConfig() != null) {
            target.setClusterConfig(copy(source.getClusterConfig()));
        }
        return target;
    }

    private ClusterFlowConfig copy(ClusterFlowConfig source) {
        ClusterFlowConfig target = new ClusterFlowConfig();
        target.setFlowId(source.getFlowId());
        target.setThresholdType(source.getThresholdType());
        target.setStrategy(source.getStrategy());
        target.setFallbackToLocalWhenFail(source.isFallbackToLocalWhenFail());
        target.setSampleCount(source.getSampleCount());
        target.setWindowIntervalMs(source.getWindowIntervalMs());
        target.setResourceTimeout(source.getResourceTimeout());
        target.setResourceTimeoutStrategy(source.getResourceTimeoutStrategy());
        target.setAcquireRefuseStrategy(source.getAcquireRefuseStrategy());
        target.setClientOfflineTime(source.getClientOfflineTime());
        return target;
    }

    /**
     * 当前生效规则中集群规则的条数，用于巡检接口展示。
     */
    public static long countClusterRules(List<FlowRule> rules) {
        return rules == null ? 0 : rules.stream()
                .filter(FlowRule::isClusterMode)
                .filter(rule -> Objects.nonNull(rule.getClusterConfig()))
                .count();
    }
}
