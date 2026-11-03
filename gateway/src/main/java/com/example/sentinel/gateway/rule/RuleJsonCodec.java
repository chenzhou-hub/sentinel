package com.example.sentinel.gateway.rule;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 流控规则的 JSON 编解码。
 *
 * <p>Nacos 中的规则由 token-server 与本应用共用，所以序列化/反序列化必须保持字段兼容
 * （resource、grade、count、clusterMode、clusterConfig 等）。</p>
 */
@Component
public class RuleJsonCodec {

    private static final TypeReference<List<FlowRule>> FLOW_RULE_LIST = new TypeReference<>() {
    };

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public List<FlowRule> parse(String source) {
        if (!StringUtils.hasText(source)) {
            return new ArrayList<>();
        }
        try {
            List<FlowRule> rules = objectMapper.readValue(source, FLOW_RULE_LIST);
            return rules == null ? new ArrayList<>() : rules;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to parse flow rules: " + e.getMessage(), e);
        }
    }

    public String write(List<FlowRule> rules) {
        try {
            return objectMapper.writeValueAsString(rules == null ? List.of() : rules);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize flow rules", e);
        }
    }

    /**
     * 规范化内容（解析后再序列化），用于比较两份规则是否等价，避免空白/字段顺序差异导致误判。
     */
    public String normalize(String source) {
        return write(parse(source));
    }
}
