package com.example.sentinel.tokenserver.rule;

import java.util.List;

import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.example.sentinel.tokenserver.config.NacosProperties;
import com.example.sentinel.tokenserver.config.TokenServerProperties;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NacosRulePropertySupplierTest {

    private final NacosRulePropertySupplier supplier =
            new NacosRulePropertySupplier(new NacosProperties(), new TokenServerProperties.Rule());

    @Test
    void shouldParseClusterFlowRule() {
        String json = "[{\"resource\":\"/api/order\",\"limitApp\":\"default\",\"grade\":1,\"count\":100,"
                + "\"strategy\":0,\"controlBehavior\":0,\"clusterMode\":true,"
                + "\"clusterConfig\":{\"flowId\":10001,\"thresholdType\":0,\"strategy\":0,"
                + "\"fallbackToLocalWhenFail\":true}}]";

        List<FlowRule> rules = supplier.parseFlowRules(json);

        assertThat(rules).hasSize(1);
        FlowRule rule = rules.get(0);
        assertThat(rule.getResource()).isEqualTo("/api/order");
        assertThat(rule.isClusterMode()).isTrue();
        assertThat(rule.getCount()).isEqualTo(100D);
        assertThat(rule.getClusterConfig().getFlowId()).isEqualTo(10001L);
        assertThat(rule.getClusterConfig().isFallbackToLocalWhenFail()).isTrue();
    }

    @Test
    void shouldReturnEmptyListWhenConfigIsBlank() {
        assertThat(supplier.parseFlowRules(null)).isEmpty();
        assertThat(supplier.parseFlowRules("  ")).isEmpty();
    }

    @Test
    void shouldThrowWhenConfigIsInvalid() {
        assertThatThrownBy(() -> supplier.parseFlowRules("{not-json"))
                .isInstanceOf(IllegalStateException.class);
    }
}
