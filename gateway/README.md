# sentinel-gateway

Spring Cloud Gateway + Sentinel **集群流控客户端**，与 `../token-server` 配套使用：

- 网关作为 Token Client，从 Nacos 发现 token-server，令牌统计集中在 token-server；
- 规则与 token-server 共用同一份 Nacos 配置（`{namespace}-flow-rules` @ `SENTINEL_GROUP`）；
- **Sentinel Dashboard 负责日常配置**：在 Dashboard 上按普通流控规则操作，网关自动补全集群配置并回写 Nacos，token-server 立即生效。

## 1. 为什么需要这个工程

`token-server` 只解决了"令牌服务 + 服务发现 + 规则拉取"，它没有流量入口，也不接受没有集群配置的规则：

| token-server 的约束 | 网关侧的对应实现 |
| --- | --- |
| Token Client 需通过 Nacos 发现实例（`metadata.namespaces` 过滤） | `TokenServerDiscovery` 订阅 Nacos 命名服务，动态 `registerServerAssignProperty` |
| 只认 `clusterMode=true` 且带 `clusterConfig.flowId` 的规则 | `ClusterRuleMerger` 自动补全 `clusterMode` / `flowId` / `thresholdType` |
| 规则从 Nacos `{namespace}-flow-rules` 拉取 | `FlowRuleSyncManager` 读同一份配置，并支持回写 |
| namespace 即客户端应用名 | 应用名 `api-gateway` 必须出现在 token-server 的 `namespace-set` |
| 没有可视化管理界面 | 通过 `sentinel-transport-simple-http` 接入 Dashboard |

## 2. 架构与数据流

```
                ┌───────────────────────┐
   HTTP  ─────► │  gateway (8082)      │ ── 路由转发 ──► 后端服务 / mock
                │  Spring Cloud Gateway │
                │  + Sentinel SCG 适配器 │
                └───────┬───────────────┘
                        │ ① 集群令牌请求（Netty，18730）
                        │ ② 从 Nacos 发现 token-server
                        ▼
                ┌───────────────────────┐          ┌───────────────────────┐
                │ token-server (18730)  │ ◄─────── │  Nacos (8848)         │
                │ 集群令牌 & 阈值统计     │  ④ 拉规则 │  api-gateway-flow-rules│
                └───────────────────────┘          └───────────▲───────────┘
                                                           ③ 回写
                ┌───────────────────────┐          ┌───────────┴───────────┐
                │ Sentinel Dashboard     │ ──下发──► │ gateway 规则同步线程   │
                │ (8858)                │  规则     │ 补全 clusterConfig     │
                └───────────────────────┘          └───────────────────────┘
```

规则同步时序（与代码一致）：

1. 启动：`NacosDataSource` 读取 `api-gateway-flow-rules` → 写入 `FlowRuleManager`；
2. 启动：`TokenServerDiscovery` 选出承载 `api-gateway` 命名空间的 token-server，注册地址并 `applyState(CLUSTER_CLIENT)`；
3. Dashboard 编辑规则 → `POST /setRules?type=flow` 打到网关命令端口 `18719`；
4. 网关单线程合并：补全 `clusterMode=true` + `clusterConfig`（复用同 resource 的历史 `flowId`）→ 本地重新生效 → 内容与 Nacos 不同则回写；
5. token-server 收到 Nacos 推送 → `ClusterFlowRuleManager` 更新集群阈值；
6. 后续请求由网关向 token-server 申请令牌，超阈值直接返回 429。

## 3. 端口规划

| 端口 | 归属 | 用途 |
| --- | --- | --- |
| 8082 | gateway | 对外网关端口 |
| 18719 | gateway | Sentinel 命令端口（Dashboard 下发规则、拉取规则） |
| 8858 | dashboard | 控制台（**不要用 8080**，Nacos 控制台默认占用） |
| 18730 | token-server | Token Server（Netty，网关直连） |
| 18731 | token-server | token-server 管理端口 |
| 18732 | token-server | token-server 的 Sentinel 命令端口 |
| 8848 / 9848 | Nacos | 服务端 API / gRPC |

## 4. 启动顺序

### 4.1 Nacos

Nacos 需已启动（默认 `localhost:8848`）。本机使用 Nacos 3.3 时，**nacos-client 必须 ≥ 2.5.x**，
否则注册与配置订阅都会报 `Code: 401, Message: User not found!`。
本工程与 `token-server`（`pom.xml` 中 `nacos-client.version`）都已改为 `2.5.1`。

### 4.2 Dashboard

```bash
java -Dserver.port=8858 \
     -Dcsp.sentinel.dashboard.server=localhost:8858 \
     -Dproject.name=sentinel-dashboard \
     -jar sentinel-dashboard-1.8.10.jar
```

默认账号密码 `sentinel / sentinel`。

### 4.3 token-server

```bash
# 关键：namespace 必须与网关的应用名一致
$env:SENTINEL_TOKEN_NAMESPACE='api-gateway'
java -jar token-server/target/token-server-1.0.0.jar
```

校验：

```bash
curl http://localhost:18731/token-server/info
# {"port":18730,...,"nacosRegistered":true,"namespaces":{"default":{...},"api-gateway":{...}}}
```

### 4.4 gateway

```bash
cd gateway
mvn clean package -DskipTests
java -jar target/gateway-1.0.0.jar
```

或开发态：

```bash
mvn spring-boot:run
```

校验：

```bash
curl http://localhost:8082/actuator/health
curl http://localhost:8082/gateway-sentinel/info
```

`/gateway-sentinel/info` 返回示例：

```json
{
  "appName": "api-gateway",
  "namespace": "api-gateway",
  "dashboard": "127.0.0.1:8858",
  "apiPort": 18719,
  "nacos": {
    "registered": true,
    "serviceName": "sentinel-gateway",
    "group": "DEFAULT_GROUP",
    "address": "192.168.0.20:8082"
  },
  "cluster": {
    "enabled": true,
    "state": "CLUSTER_CLIENT",
    "clusterClientStarted": true,
    "tokenServerHost": "192.168.0.20",
    "tokenServerPort": 18730,
    "matchedInstanceCount": 1,
    "requestTimeoutMs": 20
  },
  "rule": {
    "dataId": "api-gateway-flow-rules",
    "group": "SENTINEL_GROUP",
    "ruleCount": 1,
    "clusterRuleCount": 1,
    "autoClusterEnabled": true,
    "publishEnabled": true,
    "lastSyncSource": "dashboard",
    "lastSyncTime": 1793709627377
  }
}
```

### 4.5 在 Nacos 服务列表确认网关

网关启动后会以 `nacos.discovery.service-name`（默认 `sentinel-gateway`）注册为临时实例，
在 Nacos 控制台「服务管理 → 服务列表」中可以看到，实例元数据包含 `app`、`sentinel.namespace`、
`sentinel.apiPort`、`sentinel.dashboard` 等字段。

```bash
# 服务列表（应同时出现 sentinel-token-server 与 sentinel-gateway）
curl "http://localhost:8848/nacos/v3/admin/ns/service/list?namespaceId=public&groupName=DEFAULT_GROUP&pageNo=1&pageSize=20" \
     -H "accessToken: <登录拿到的 token>"
```

说明：

- 该注册**只用于让网关出现在 Nacos 服务列表 / 被其他组件发现**；Sentinel 集群限流找 token-server 走的是
  `sentinel.cluster.service-name`，两者互不影响；
- 注册失败（例如 Nacos 比网关启动晚）不会阻塞启动，后台每 5 秒重试一次；
- 进程正常退出时自动注销；被 `kill -9` 强杀时由 Nacos 心跳超时（约 15 秒）剔除；
- 关闭注册：`NACOS_DISCOVERY_ENABLED=false`。

> `lb://服务名` 路由需要 Spring Cloud LoadBalancer + DiscoveryClient。当前工程只做了「注册」，
> 没有引入 `spring-cloud-starter-alibaba-nacos-discovery` / `spring-cloud-starter-loadbalancer`，
> 所以路由请用 `http://` / `forward:` 直连；如需 `lb://`，需要补充这两个依赖。

## 5. 用 Dashboard 管理配置

1. 登录 Dashboard，进入「流控规则」→ 左侧应用列表应能看到 `api-gateway`；
2. 新增规则：
   - **资源名 = 网关路由 id**（本工程演示路由为 `order-api`，不是 URL）；
   - 阈值类型 QPS / 线程数，阈值、流控效果按普通规则填写；
   - 不需要（也没有）集群模式开关，网关会自动补全；
3. 保存后 1~2 秒内：
   - Dashboard 规则列表刷新，可见该规则已带 `clusterMode`；
   - `curl http://localhost:8082/gateway-sentinel/rules` 能看到 `clusterConfig`；
   - Nacos 配置 `api-gateway-flow-rules`（group `SENTINEL_GROUP`）内容被更新；
   - `curl http://localhost:18731/token-server/info` 中 `api-gateway.flowRules` 数量增加；
4. 「集群流控」页面可查看 Token Client/Server 连接与规则状态。

压测验证（演示阈值 3 QPS）：

```bash
for i in $(seq 1 10); do curl -s -o /dev/null -w "%{http_code} " http://localhost:8082/api/order; done
# 200 200 200 429 429 429 429 429 429 429
```

被限流时返回：

```json
{
  "code": 429,
  "message": "Too many requests, blocked by Sentinel",
  "path": "/api/order",
  "route": "order-api",
  "resource": "order-api",
  "ruleLimitApp": "default"
}
```

## 6. 配置说明（application.yml）

| 配置项 | 说明 |
| --- | --- |
| `spring.application.name` | 应用名，同时作为集群 namespace，必须与 token-server 的 `namespace-set` 一致 |
| `spring.cloud.gateway.routes[*]` | 路由定义；`id` 就是 Sentinel 资源名 |
| `sentinel.dashboard` | Dashboard 地址，留空则不上报（不影响限流） |
| `sentinel.api-port` | Sentinel 命令端口，默认 18719 |
| `sentinel.cluster.enabled` | 是否作为 Token Client；false 时只做单机限流 |
| `sentinel.cluster.namespace` | 集群命名空间，默认 `${spring.application.name}` |
| `sentinel.cluster.service-name` / `group` | token-server 在 Nacos 中的服务名/分组 |
| `sentinel.cluster.request-timeout` | 申请令牌超时（ms），默认 20 |
| `sentinel.cluster.fail-fast` | 启动找不到 token-server 是否直接失败；默认 false（先本地限流） |
| `sentinel.rule.data-id-pattern` | 规则 dataId 模板，需与 token-server 完全一致 |
| `sentinel.rule.group` | 规则分组，需与 token-server 完全一致 |
| `sentinel.rule.publish-enabled` | 是否把规则回写 Nacos（关闭后 Dashboard 的规则只在网关本地生效） |
| `sentinel.rule.auto-cluster-enabled` | 是否自动补全 `clusterConfig`（关闭时只接受 Nacos 中已带集群配置的规则） |
| `sentinel.rule.threshold-type` | 0 单机均摊、1 全局阈值 |
| `sentinel.rule.fallback-to-local-when-fail` | token-server 异常时是否退化为本地限流 |
| `sentinel.rule.flow-id-base` / `flow-id-mod` | 自动生成 flowId 的范围：`base + crc32(resource) % mod` |
| `nacos.*` | Nacos 地址、命名空间、账号密码（同时用于服务发现与配置读写） |
| `nacos.discovery.enabled` | 是否把网关自身注册到 Nacos 服务列表 |
| `nacos.discovery.service-name` / `group` | 注册的服务名/分组，默认 `sentinel-gateway` / `DEFAULT_GROUP` |
| `nacos.discovery.ip` / `weight` / `ephemeral` | 注册 IP（留空自动探测内网 IPv4）、权重、是否临时实例 |
| `nacos.discovery.metadata.*` | 注册实例的附加元数据 |

环境变量覆盖示例：

```bash
SERVER_PORT=8082
SENTINEL_DASHBOARD=127.0.0.1:8858
SENTINEL_CLUSTER_NAMESPACE=api-gateway
NACOS_SERVER_ADDR=localhost:8848
NACOS_USERNAME=nacos
NACOS_PASSWORD=***
NACOS_SERVICE_NAME=sentinel-gateway
```

## 7. Nacos 中的规则格式

网关回写/手工维护的格式与 token-server README 一致：

```json
[
  {
    "resource": "order-api",
    "limitApp": "default",
    "grade": 1,
    "count": 3,
    "strategy": 0,
    "controlBehavior": 0,
    "warmUpPeriodSec": 10,
    "maxQueueingTimeMs": 500,
    "clusterMode": true,
    "clusterConfig": {
      "flowId": 1,
      "thresholdType": 0,
      "strategy": 0,
      "fallbackToLocalWhenFail": true,
      "sampleCount": 10,
      "windowIntervalMs": 1000
    }
  }
]
```

要点：

- `resource` 必须是路由 id；
- `flowId` 全局唯一、正数；网关自动生成时会保持稳定（同一 resource 复用历史值）；
- `thresholdType=0`：总阈值 = 单机阈值 × 已连接客户端数；`1`：固定全局阈值；
- 手工在 Nacos 修改后，网关与 token-server 都会实时收到推送，无需重启。

## 8. 实现要点

| 类 | 职责 |
| --- | --- |
| `SentinelGatewayConfiguration` | 注册 SCG 适配器 `SentinelGatewayFilter` / 异常处理器，统一 429 JSON |
| `TokenServerDiscovery` | Nacos 订阅 token-server，按 `namespaces` metadata 过滤，动态设置地址并切换 `CLUSTER_CLIENT` |
| `ClusterRuleMerger` | 补全集群配置、复用/生成 flowId，纯函数、可单测 |
| `FlowRuleSyncManager` | 规则装载、单线程合并（避免在 property 回调里覆盖规则）、回写 Nacos、防自激循环 |
| `RuleJsonCodec` | 与 token-server 兼容的规则 JSON 编解码 |
| `GatewaySentinelInfoController` | `/gateway-sentinel/info`、`/gateway-sentinel/rules` 巡检 |

防回环设计：只在"合并后内容 ≠ Nacos 当前内容 ≠ 上次回写内容"时才写入，Nacos 推送回来时内容一致，直接跳过。

## 9. 常见问题

1. **Dashboard 上看不到 api-gateway**：确认 `sentinel.dashboard` 正确、`18719` 端口未被防火墙拦截；应用停止后 1~2 分钟会从列表消失。
2. **规则保存了但 token-server 没有**：检查 `sentinel.rule.publish-enabled=true`、网关日志是否有 `Flow rules published to Nacos`、dataId/group 是否与 token-server 一致。
3. **限流不生效、日志有 `no rules in token server`**：token-server 的 `namespace-set` 未包含网关应用名，或规则没有 `clusterMode=true`（确认 `auto-cluster-enabled`）。
4. **Nacos 401 `User not found`**：Nacos 3.x 需要 nacos-client ≥ 2.5.x；同时确认 `NACOS_USERNAME` / `NACOS_PASSWORD`。
5. **Dashboard 端口冲突**：Nacos 控制台默认 8080，Dashboard 建议换 8858。
6. **Dashboard 保存后 flowId 变化**：Dashboard 保存规则时会按规则 id 重整集群配置，网关会把新 flowId 原样同步给 token-server，两侧仍一致；如需固定 flowId，请直接在 Nacos 中维护规则（关闭 `publish-enabled` 或按需调整）。
7. **一个 namespace 只能有一个 token-server 实例**：集群令牌统计是单机内存态，多实例需按 namespace 分片。
8. **Dashboard 规则不持久化**：Dashboard 本身不存规则，以网关/Nacos 为准；网关重启后从 Nacos 恢复。

## 10. Docker

```bash
docker build -t sentinel-gateway:1.0.0 .
docker run -d --name gateway -p 8082:8082 -p 18719:18719 \
  -e NACOS_SERVER_ADDR=192.168.0.10:8848 \
  -e SENTINEL_DASHBOARD=192.168.0.10:8858 \
  -e SENTINEL_CLUSTER_NAMESPACE=api-gateway \
  sentinel-gateway:1.0.0
```
