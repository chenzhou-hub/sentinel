package com.example.sentinel.gateway.rule;

import java.util.List;

import com.alibaba.csp.sentinel.slots.block.RuleConstant;
import com.alibaba.csp.sentinel.slots.block.flow.ClusterFlowConfig;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.example.sentinel.gateway.config.GatewaySentinelProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterRuleMergerTest {

    private final GatewaySentinelProperties properties = new GatewaySentinelProperties();

    private final ClusterRuleMerger merger = new ClusterRuleMerger(properties);

    @Test
    void shouldAppendClusterConfigToPlainRule() {
        FlowRule rule = plainRule("order-api", 10);

        List<FlowRule> merged = merger.merge(List.of(rule), List.of());

        assertThat(merged).hasSize(1);
        FlowRule result = merged.get(0);
        assertThat(result.isClusterMode()).isTrue();
        assertThat(result.getClusterConfig()).isNotNull();
        assertThat(result.getClusterConfig().getFlowId()).isPositive();
        assertThat(result.getClusterConfig().getThresholdType()).isZero();
        assertThat(result.getClusterConfig().isFallbackToLocalWhenFail()).isTrue();
        assertThat(result.getClusterConfig().getSampleCount()).isEqualTo(10);
        assertThat(result.getClusterConfig().getWindowIntervalMs()).isEqualTo(1000);
        // 原规则对象不应被修改
        assertThat(rule.isClusterMode()).isFalse();
    }

    @Test
    void shouldKeepFlowIdStableForSameResource() {
        FlowRule first = merger.merge(List.of(plainRule("order-api", 10)), List.of()).get(0);
        FlowRule second = merger.merge(List.of(plainRule("order-api", 20)), List.of()).get(0);

        assertThat(second.getClusterConfig().getFlowId())
                .isEqualTo(first.getClusterConfig().getFlowId());
    }

    @Test
    void shouldReusePreviousFlowIdWhenDashboardDropsClusterConfig() {
        FlowRule previous = merger.merge(List.of(plainRule("order-api", 10)), List.of()).get(0);
        previous.getClusterConfig().setFlowId(9527L);

        FlowRule fromDashboard = plainRule("order-api", 50);
        List<FlowRule> merged = merger.merge(List.of(fromDashboard), List.of(previous));

        assertThat(merged.get(0).getClusterConfig().getFlowId()).isEqualTo(9527L);
        assertThat(merged.get(0).getCount()).isEqualTo(50D);
    }

    @Test
    void shouldKeepExistingClusterRuleUntouched() {
        FlowRule rule = plainRule("order-api", 10);
        rule.setClusterMode(true);
        rule.setClusterConfig(new ClusterFlowConfig()
                .setFlowId(10001L)
                .setThresholdType(1)
                .setStrategy(1)
                .setFallbackToLocalWhenFail(false)
                .setSampleCount(5)
                .setWindowIntervalMs(500));

        FlowRule merged = merger.merge(List.of(rule), List.of()).get(0);

        assertThat(merged.getClusterConfig().getFlowId()).isEqualTo(10001L);
        assertThat(merged.getClusterConfig().getThresholdType()).isEqualTo(1);
        assertThat(merged.getClusterConfig().getSampleCount()).isEqualTo(5);
        assertThat(merged.getClusterConfig().isFallbackToLocalWhenFail()).isFalse();
    }

    @Test
    void shouldDoNothingWhenClusterDisabled() {
        properties.getCluster().setEnabled(false);

        FlowRule merged = merger.merge(List.of(plainRule("order-api", 10)), List.of()).get(0);

        assertThat(merged.isClusterMode()).isFalse();
        assertThat(merged.getClusterConfig()).isNull();
    }

    private FlowRule plainRule(String resource, double count) {
        FlowRule rule = new FlowRule(resource);
        rule.setGrade(RuleConstant.FLOW_GRADE_QPS);
        rule.setCount(count);
        rule.setLimitApp("default");
        return rule;
    }
}
