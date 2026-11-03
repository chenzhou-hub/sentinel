package com.example.sentinel.gateway.rule;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.ConfigType;
import com.example.sentinel.gateway.config.NacosProperties;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 把规则写回 Nacos 配置中心，供 token-server 拉取集群规则。
 */
@Component
public class NacosRulePublisher {

    private static final Logger log = LoggerFactory.getLogger(NacosRulePublisher.class);

    private final NacosProperties nacosProperties;

    private volatile ConfigService configService;

    public NacosRulePublisher(NacosProperties nacosProperties) {
        this.nacosProperties = nacosProperties;
    }

    public void publish(String dataId, String group, String content) {
        try {
            boolean success = configService().publishConfig(dataId, group, content, ConfigType.JSON.getType());
            if (!success) {
                throw new IllegalStateException("Nacos publishConfig returned false");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to publish flow rules to Nacos, dataId=" + dataId
                    + ", group=" + group, e);
        }
    }

    private ConfigService configService() throws Exception {
        ConfigService service = this.configService;
        if (service == null) {
            synchronized (this) {
                service = this.configService;
                if (service == null) {
                    service = NacosFactory.createConfigService(nacosProperties.toProperties());
                    this.configService = service;
                    log.info("[Gateway] Nacos config service created, serverAddr={}", nacosProperties.getServerAddr());
                }
            }
        }
        return service;
    }

    @PreDestroy
    public void destroy() {
        ConfigService service = this.configService;
        if (service != null) {
            try {
                service.shutDown();
            } catch (Exception e) {
                log.warn("[Gateway] Failed to shutdown Nacos config service", e);
            }
        }
    }
}
