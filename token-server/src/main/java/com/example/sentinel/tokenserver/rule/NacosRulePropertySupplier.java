package com.example.sentinel.tokenserver.rule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.csp.sentinel.datasource.nacos.NacosDataSource;
import com.alibaba.csp.sentinel.property.SentinelProperty;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.param.ParamFlowRule;
import com.example.sentinel.tokenserver.config.NacosProperties;
import com.example.sentinel.tokenserver.config.TokenServerProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

/**
 * 按 namespace 从 Nacos 拉取集群规则。
 *
 * <p>Token Server 的集群规则是"命名空间维度"的，Sentinel 要求为每个 namespace
 * 提供一个 {@link SentinelProperty}，因此这里用 dataId 模板把 namespace 拼进 dataId。</p>
 */
public class NacosRulePropertySupplier {

    private static final Logger log = LoggerFactory.getLogger(NacosRulePropertySupplier.class);

    private static final String NAMESPACE_PLACEHOLDER = "{namespace}";

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Properties nacosProperties;

    private final TokenServerProperties.Rule ruleProperties;

    private final Map<String, NacosDataSource<List<FlowRule>>> flowRuleDataSources = new ConcurrentHashMap<>();

    private final Map<String, NacosDataSource<List<ParamFlowRule>>> paramFlowRuleDataSources =
            new ConcurrentHashMap<>();

    public NacosRulePropertySupplier(NacosProperties nacosProperties,
                                     TokenServerProperties.Rule ruleProperties) {
        this.nacosProperties = nacosProperties.toProperties();
        this.ruleProperties = ruleProperties;
    }

    /**
     * 集群流控规则属性，供 {@code ClusterFlowRuleManager.setPropertySupplier} 使用。
     */
    public SentinelProperty<List<FlowRule>> flowRuleProperty(String namespace) {
        return flowRuleDataSources.computeIfAbsent(namespace, ns -> {
            String dataId = resolveDataId(ruleProperties.getDataIdPattern(), ns);
            log.info("[TokenServer] Registering cluster flow rule datasource, namespace={}, dataId={}, group={}",
                    ns, dataId, ruleProperties.getGroup());
            return new NacosDataSource<>(nacosProperties, ruleProperties.getGroup(), dataId,
                    this::parseFlowRules);
        }).getProperty();
    }

    /**
     * 集群热点参数规则属性，供 {@code ClusterParamFlowRuleManager.setPropertySupplier} 使用。
     */
    public SentinelProperty<List<ParamFlowRule>> paramFlowRuleProperty(String namespace) {
        return paramFlowRuleDataSources.computeIfAbsent(namespace, ns -> {
            String dataId = resolveDataId(ruleProperties.getParamDataIdPattern(), ns);
            log.info("[TokenServer] Registering cluster param flow rule datasource, namespace={}, dataId={}, group={}",
                    ns, dataId, ruleProperties.getGroup());
            return new NacosDataSource<>(nacosProperties, ruleProperties.getGroup(), dataId,
                    this::parseParamFlowRules);
        }).getProperty();
    }

    private String resolveDataId(String pattern, String namespace) {
        return pattern.replace(NAMESPACE_PLACEHOLDER, namespace);
    }

    List<FlowRule> parseFlowRules(String source) {
        return parseList(source, new TypeReference<List<FlowRule>>() {
        }, "flow");
    }

    List<ParamFlowRule> parseParamFlowRules(String source) {
        return parseList(source, new TypeReference<List<ParamFlowRule>>() {
        }, "param-flow");
    }

    /**
     * 解析失败时抛出异常，Sentinel 会保留上一次生效的规则，避免空规则误放大流量。
     */
    private <T> List<T> parseList(String source, TypeReference<List<T>> type, String ruleType) {
        if (!StringUtils.hasText(source)) {
            return new ArrayList<>();
        }
        try {
            return objectMapper.readValue(source, type);
        } catch (Exception e) {
            log.error("[TokenServer] Failed to parse cluster {} rules from Nacos, keep previous rules", ruleType, e);
            throw new IllegalStateException("Failed to parse cluster " + ruleType + " rules", e);
        }
    }
}
