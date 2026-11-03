package com.example.sentinel.gateway.rule;

import java.util.List;

import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RuleJsonCodecTest {

    private final RuleJsonCodec codec = new RuleJsonCodec();

    @Test
    void shouldParseClusterFlowRuleFromNacos() {
        String json = "[{\"resource\":\"order-api\",\"limitApp\":\"default\",\"grade\":1,\"count\":100,"
                + "\"strategy\":0,\"controlBehavior\":0,\"clusterMode\":true,"
                + "\"clusterConfig\":{\"flowId\":10001,\"thresholdType\":0,\"strategy\":0,"
                + "\"fallbackToLocalWhenFail\":true,\"sampleCount\":10,\"windowIntervalMs\":1000}}]";

        List<FlowRule> rules = codec.parse(json);

        assertThat(rules).hasSize(1);
        FlowRule rule = rules.get(0);
        assertThat(rule.getResource()).isEqualTo("order-api");
        assertThat(rule.isClusterMode()).isTrue();
        assertThat(rule.getClusterConfig().getFlowId()).isEqualTo(10001L);
        assertThat(rule.getClusterConfig().getSampleCount()).isEqualTo(10);
    }

    @Test
    void shouldRoundTripThroughJson() {
        FlowRule rule = new FlowRule();
        rule.setResource("order-api");
        rule.setLimitApp("default");
        rule.setGrade(1);
        rule.setCount(20);
        rule.setClusterMode(true);
        rule.setClusterConfig(new com.alibaba.csp.sentinel.slots.block.flow.ClusterFlowConfig()
                .setFlowId(20002L)
                .setThresholdType(0)
                .setFallbackToLocalWhenFail(true)
                .setSampleCount(10)
                .setWindowIntervalMs(1000));

        String json = codec.write(List.of(rule));
        List<FlowRule> parsed = codec.parse(json);

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).getClusterConfig().getFlowId()).isEqualTo(20002L);
        assertThat(codec.write(parsed)).isEqualTo(json);
    }

    @Test
    void shouldTreatBlankContentAsEmptyRules() {
        assertThat(codec.parse(null)).isEmpty();
        assertThat(codec.parse("  ")).isEmpty();
        assertThat(codec.write(List.of())).isEqualTo("[]");
    }

    @Test
    void shouldThrowWhenContentIsInvalid() {
        assertThatThrownBy(() -> codec.parse("{not-json"))
                .isInstanceOf(IllegalStateException.class);
    }
}
