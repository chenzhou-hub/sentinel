package com.example.sentinel.gateway.rule;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.csp.sentinel.datasource.nacos.NacosDataSource;
import com.alibaba.csp.sentinel.property.DynamicSentinelProperty;
import com.alibaba.csp.sentinel.property.PropertyListener;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
import com.example.sentinel.gateway.config.GatewaySentinelProperties;
import com.example.sentinel.gateway.config.NacosProperties;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 规则同步编排：
 *
 * <pre>
 * Nacos 配置 ──► FlowRuleManager（本地生效）
 *                    ▲
 * Dashboard ──/setRules──► 上面的 property
 *                    │
 *                    └──► 补全 clusterConfig ──► 回写 Nacos ──► token-server 生效
 * </pre>
 *
 * <p>回写保证了在 Dashboard 上只需要按普通流控规则的方式操作（resource 填路由 id），
 * 集群阈值就能同步给 token-server，不需要改造 Dashboard。</p>
 */
@Component
public class FlowRuleSyncManager implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(FlowRuleSyncManager.class);

    private static final String SOURCE_NACOS = "nacos";

    private static final String SOURCE_NACOS_INITIAL = "nacos-initial";

    private static final String SOURCE_RULE_PROPERTY = "rule-property";

    private static final String SOURCE_DASHBOARD = "dashboard";

    private final GatewaySentinelProperties properties;

    private final NacosProperties nacosProperties;

    private final RuleJsonCodec codec;

    private final ClusterRuleMerger merger;

    private final NacosRulePublisher publisher;

    private final String appName;

    /**
     * 注册给 FlowRuleManager 的属性，Dashboard 的 /setRules 最终会更新到它上面。
     */
    private final DynamicSentinelProperty<List<FlowRule>> ruleProperty = new DynamicSentinelProperty<>();

    /**
     * 规则合并在单线程里执行：避免在 property 的监听回调里再次更新同一个 property，
     * 否则监听器会拿到未补全 clusterConfig 的规则并覆盖已生效的规则。
     */
    private final ExecutorService syncExecutor = Executors.newSingleThreadExecutor(runnable -> {
        Thread thread = new Thread(runnable, "sentinel-rule-sync");
        thread.setDaemon(true);
        return thread;
    });

    private final AtomicBoolean syncScheduled = new AtomicBoolean(false);

    private volatile PendingUpdate pendingUpdate;

    private volatile List<FlowRule> effectiveRules = List.of();

    private volatile NacosDataSource<List<FlowRule>> dataSource;

    private volatile String dataId;

    private volatile String lastNacosContent = "";

    private volatile String lastPublishedContent = "";

    private volatile long lastSyncTime;

    private volatile String lastSyncSource = "none";

    public FlowRuleSyncManager(GatewaySentinelProperties properties,
                               NacosProperties nacosProperties,
                               RuleJsonCodec codec,
                               ClusterRuleMerger merger,
                               NacosRulePublisher publisher,
                               @Value("${spring.application.name}") String appName) {
        this.properties = properties;
        this.nacosProperties = nacosProperties;
        this.codec = codec;
        this.merger = merger;
        this.publisher = publisher;
        this.appName = appName;
    }

    @PostConstruct
    public void registerRuleProperty() {
        FlowRuleManager.register2Property(ruleProperty);
        ruleProperty.addListener(new PropertyListener<>() {
            @Override
            public void configUpdate(List<FlowRule> value) {
                scheduleSync(value, SOURCE_RULE_PROPERTY);
            }

            @Override
            public void configLoad(List<FlowRule> value) {
                scheduleSync(value, SOURCE_RULE_PROPERTY);
            }
        });
    }

    @Override
    public void run(ApplicationArguments args) {
        GatewaySentinelProperties.Rule ruleProperties = properties.getRule();
        if (!ruleProperties.isEnabled()) {
            log.warn("[Gateway] Nacos flow rule datasource is disabled, rules can only be pushed by dashboard");
            return;
        }
        this.dataId = ruleProperties.getDataIdPattern()
                .replace("{namespace}", properties.getCluster().resolveNamespace(appName));
        try {
            NacosDataSource<List<FlowRule>> source = new NacosDataSource<>(nacosProperties.toProperties(),
                    ruleProperties.getGroup(), dataId, config -> {
                        List<FlowRule> rules = codec.parse(config);
                        lastNacosContent = codec.write(rules);
                        return rules;
                    });
            source.getProperty().addListener(new PropertyListener<>() {
                @Override
                public void configUpdate(List<FlowRule> value) {
                    scheduleSync(value, SOURCE_NACOS);
                }

                @Override
                public void configLoad(List<FlowRule> value) {
                    scheduleSync(value, SOURCE_NACOS);
                }
            });
            this.dataSource = source;
            applyInitialRules(source);
            log.info("[Gateway] Nacos flow rule datasource registered, dataId={}, group={}",
                    dataId, ruleProperties.getGroup());
        } catch (Exception e) {
            log.error("[Gateway] Failed to init Nacos flow rule datasource, dataId={}, group={}",
                    dataId, ruleProperties.getGroup(), e);
        }
    }

    private void applyInitialRules(NacosDataSource<List<FlowRule>> source) {
        try {
            String initial = source.readSource();
            if (!StringUtils.hasText(initial)) {
                log.info("[Gateway] No flow rules found in Nacos yet, dataId={}", dataId);
                return;
            }
            List<FlowRule> rules = codec.parse(initial);
            lastNacosContent = codec.write(rules);
            scheduleSync(rules, SOURCE_NACOS_INITIAL);
        } catch (Exception e) {
            log.error("[Gateway] Failed to load initial flow rules from Nacos, dataId={}", dataId, e);
        }
    }

    private void scheduleSync(List<FlowRule> rules, String source) {
        pendingUpdate = new PendingUpdate(rules, source);
        if (syncScheduled.compareAndSet(false, true)) {
            syncExecutor.execute(this::drainPendingUpdates);
        }
    }

    private void drainPendingUpdates() {
        try {
            PendingUpdate update;
            while ((update = pendingUpdate) != null) {
                pendingUpdate = null;
                synchronize(update.rules(), update.source());
            }
        } finally {
            syncScheduled.set(false);
            if (pendingUpdate != null && syncScheduled.compareAndSet(false, true)) {
                syncExecutor.execute(this::drainPendingUpdates);
            }
        }
    }

    private void synchronize(List<FlowRule> incoming, String source) {
        try {
            List<FlowRule> merged = merger.merge(incoming, effectiveRules);
            String incomingContent = codec.write(incoming);
            String mergedContent = codec.write(merged);
            boolean clusterConfigAppended = !mergedContent.equals(incomingContent);
            // 启动时从 Nacos 读到的规则通常已带 clusterConfig，此时也必须写进 FlowRuleManager，
            // 否则重启后规则会"看得见但没生效"
            List<FlowRule> current = FlowRuleManager.getRules();
            boolean ruleSetChanged = !mergedContent.equals(codec.write(current == null ? List.of() : current));
            effectiveRules = merged;
            lastSyncTime = System.currentTimeMillis();
            if (clusterConfigAppended) {
                // 能走到这里说明规则不是从 Nacos 读到的（Nacos 里的规则本来就带 clusterConfig）
                lastSyncSource = SOURCE_RULE_PROPERTY.equals(source) ? SOURCE_DASHBOARD : source;
                log.info("[Gateway] Cluster config appended to rules, resource(s)={}",
                        merged.stream().map(FlowRule::getResource).toList());
            } else if (!SOURCE_RULE_PROPERTY.equals(source)) {
                lastSyncSource = source;
            }
            // rule-property 来源本身就是 property 更新触发的，只需要在补全了 clusterConfig 时回写；
            // 其余来源（Nacos 初始化/变更）要按内容比对决定是否刷新生效规则
            if (clusterConfigAppended || (!SOURCE_RULE_PROPERTY.equals(source) && ruleSetChanged)) {
                ruleProperty.updateValue(merged);
            }
            publishIfNeeded(mergedContent);
        } catch (Exception e) {
            log.error("[Gateway] Failed to synchronize flow rules", e);
        }
    }

    private void publishIfNeeded(String mergedContent) {
        GatewaySentinelProperties.Rule ruleProperties = properties.getRule();
        if (!ruleProperties.isPublishEnabled() || dataId == null) {
            return;
        }
        // 内容来自 Nacos 或刚由本应用写入：无需回写，避免自激循环
        if (mergedContent.equals(lastNacosContent) || mergedContent.equals(lastPublishedContent)) {
            return;
        }
        try {
            publisher.publish(dataId, ruleProperties.getGroup(), mergedContent);
            lastPublishedContent = mergedContent;
            log.info("[Gateway] Flow rules published to Nacos, dataId={}, group={}",
                    dataId, ruleProperties.getGroup());
        } catch (Exception e) {
            log.error("[Gateway] Failed to publish flow rules to Nacos, local rules are still effective", e);
        }
    }

    @PreDestroy
    public void destroy() {
        syncExecutor.shutdownNow();
        NacosDataSource<List<FlowRule>> source = this.dataSource;
        if (source != null) {
            try {
                source.close();
            } catch (Exception e) {
                log.warn("[Gateway] Failed to close Nacos flow rule datasource", e);
            }
        }
    }

    public String getDataId() {
        return dataId;
    }

    public String getGroup() {
        return properties.getRule().getGroup();
    }

    public List<FlowRule> getEffectiveRules() {
        return effectiveRules;
    }

    public long getClusterRuleCount() {
        return ClusterRuleMerger.countClusterRules(effectiveRules);
    }

    public long getLastSyncTime() {
        return lastSyncTime;
    }

    public String getLastSyncSource() {
        return lastSyncSource;
    }

    private record PendingUpdate(List<FlowRule> rules, String source) {
    }
}
