package com.example.sentinel.tokenserver.web;

import java.util.LinkedHashMap;
import java.util.Map;

import com.alibaba.csp.sentinel.cluster.flow.rule.ClusterFlowRuleManager;
import com.alibaba.csp.sentinel.cluster.flow.rule.ClusterParamFlowRuleManager;
import com.alibaba.csp.sentinel.cluster.server.config.ClusterServerConfigManager;
import com.example.sentinel.tokenserver.registry.NacosTokenServerRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 运维巡检接口，便于确认端口、命名空间、规则数量与 Nacos 注册状态。
 */
@RestController
@RequestMapping("/token-server")
public class TokenServerInfoController {

    private final NacosTokenServerRegistry registry;

    public TokenServerInfoController(NacosTokenServerRegistry registry) {
        this.registry = registry;
    }

    @GetMapping("/info")
    public Map<String, Object> info() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("port", ClusterServerConfigManager.getPort());
        info.put("idleSeconds", ClusterServerConfigManager.getIdleSeconds());
        info.put("maxAllowedQps", ClusterServerConfigManager.getMaxAllowedQps());
        info.put("embedded", ClusterServerConfigManager.isEmbedded());
        info.put("nacosRegistered", registry.isRegistered());
        info.put("nacosServiceName", registry.getServiceName());

        Map<String, Object> namespaces = new LinkedHashMap<>();
        for (String namespace : ClusterServerConfigManager.getNamespaceSet()) {
            Map<String, Object> detail = new LinkedHashMap<>();
            detail.put("flowRules", ClusterFlowRuleManager.getFlowIdSet(namespace).size());
            detail.put("paramFlowRules", ClusterParamFlowRuleManager.getFlowIdSet(namespace).size());
            namespaces.put(namespace, detail);
        }
        info.put("namespaces", namespaces);
        return info;
    }
}
