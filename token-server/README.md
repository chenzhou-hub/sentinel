# sentinel-token-server

Sentinel 集群流控 **Token Server（独立部署模式）**，Token Server 自身注册到 Nacos，集群规则从 Nacos 配置中心按 namespace 拉取。

## 1. 为什么需要这个工程

Sentinel 开源版的 Token Server（`sentinel-cluster-server-default`）**不带服务发现能力**：

- Token Client 只能通过 `serverHost` / `serverPort` 直连（API 或动态配置下发）；
- 官方架构里 Token Server 地址是"写死"在客户端或由配置中心下发的，没有注册中心概念。

所以本工程做了两件事：

1. **服务端**：Token Server 启动后作为临时实例（ephemeral，自动心跳）注册到 Nacos，并把承载的 `namespaces` 写进实例 metadata；
2. **客户端**（见第 6 节）：从 Nacos 取实例 → 过滤出包含自身 namespace 的实例 → 通过
   `ClusterClientConfigManager.registerServerAssignProperty` 动态设置 Token Server 地址。

## 2. 端口规划

| 端口 | 用途 | 是否必须 |
| --- | --- | --- |
| 18730 | Token Server 监听端口（Netty，Token Client 直连） | 必须 |
| 18731 | Spring Boot 管理端口（`/actuator/health`、`/token-server/info`） | 建议保留，用于探活 |
| 18732 | Sentinel 命令端口（`csp.sentinel.api.port`，默认 8719） | 仅接 Dashboard 时用到 |

## 3. 快速启动

```bash
# 1. 打包
mvn clean package -DskipTests

# 2. 关键环境变量（按实际环境覆盖）
#   NACOS_SERVER_ADDR         Nacos 服务端地址，默认 localhost:8848（8080 只是控制台端口）
#   NACOS_USERNAME/PASSWORD   Nacos 开启鉴权时必填（否则注册报 401 User not found）
#   SENTINEL_TOKEN_NAMESPACE  Token Client 的应用名（namespace），默认 demo-app
#   SENTINEL_GATEWAY_NAMESPACE 网关应用名（namespace），默认 api-gateway
#   SENTINEL_DASHBOARD        Sentinel Dashboard 地址，可留空
#   NACOS_NAMESPACE           Nacos 命名空间 ID，留空为 public
#   NACOS_REGISTER_IP         多网卡机器请显式指定注册 IP

# 3. 启动
java -jar target/token-server-1.0.0.jar
```

启动成功后可校验：

```bash
curl http://localhost:18731/actuator/health
curl http://localhost:18731/token-server/info
```

`/token-server/info` 返回示例：

```json
{
  "port": 18730,
  "idleSeconds": 600,
  "maxAllowedQps": 100000.0,
  "embedded": false,
  "nacosRegistered": true,
  "nacosServiceName": "sentinel-token-server",
  "namespaces": {
    "default": { "flowRules": 0, "paramFlowRules": 0 },
    "api-gateway": { "flowRules": 1, "paramFlowRules": 0 },
    "demo-app": { "flowRules": 0, "paramFlowRules": 0 }
  }
}
```

## 4. 配置说明（application.yml）

| 配置项 | 说明 |
| --- | --- |
| `sentinel.dashboard` | Dashboard 地址，留空则不向控制台上报（Token Server 独立运行不受影响） |
| `sentinel.api-port` | Sentinel 命令端口，默认 18732，避免与其他应用抢占 8719 |
| `sentinel.token-server.port` | Token Server 监听端口，客户端直连 |
| `sentinel.token-server.idle-seconds` | 连接最大空闲时间 |
| `sentinel.token-server.namespace-set` | 承载的命名空间集合，填 Token Client 的 `spring.application.name`；启动时自动追加 `default` |
| `sentinel.token-server.max-allowed-qps` | Token Server 自身总 QPS 保护，删除该行表示不限制 |
| `sentinel.rule.group` | 规则所在 Nacos 分组，默认 `SENTINEL_GROUP` |
| `sentinel.rule.data-id-pattern` | 集群流控规则 dataId 模板，`{namespace}` 会被替换 |
| `sentinel.rule.param-data-id-pattern` | 集群热点参数规则 dataId 模板 |
| `nacos.server-addr` / `namespace` / `username` / `password` | Nacos 连接信息（同时用于注册与规则拉取） |
| `nacos.discovery.*` | 注册开关、服务名、分组、注册 IP、权重 |

## 5. Nacos 中的规则配置

在 Nacos 配置中心新建配置（**必须与 `data-id-pattern` / `group` 对应**）：

- dataId：`demo-app-flow-rules`
- group：`SENTINEL_GROUP`
- 格式：JSON 数组

```json
[
  {
    "resource": "/api/order",
    "limitApp": "default",
    "grade": 1,
    "count": 100,
    "strategy": 0,
    "controlBehavior": 0,
    "clusterMode": true,
    "clusterConfig": {
      "flowId": 10001,
      "thresholdType": 0,
      "strategy": 0,
      "fallbackToLocalWhenFail": true
    }
  }
]
```

要点：

- `clusterMode` 必须为 `true`，否则该规则会被 Token Server 忽略；
- `flowId` 必须全局唯一且为正数（集群流控靠它区分规则）；
- `thresholdType = 0` 单机均摊（总阈值 = 单机阈值 × 已连接客户端数），`1` 为全局阈值；
- JSON 解析失败时规则不会被清空，Sentinel 保留上一次生效的规则。

## 6. Token Client 侧接入

客户端除了 `sentinel-cluster-client-default` 依赖，还需要一段"从 Nacos 发现 Token Server"的代码：

```java
@Component
public class ClusterTokenClientInitializer implements ApplicationRunner {

    private static final String SERVICE_NAME = "sentinel-token-server";
    private static final String GROUP = "DEFAULT_GROUP";

    private final NamingService namingService;
    private final String namespace;

    public ClusterTokenClientInitializer(@Value("${nacos.server-addr}") String serverAddr,
                                        @Value("${spring.application.name}") String appName) throws NacosException {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, serverAddr);
        this.namingService = NacosFactory.createNamingService(properties);
        this.namespace = appName;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Instance target = namingService.selectInstances(SERVICE_NAME, GROUP, true).stream()
                .filter(i -> Arrays.asList(i.getMetadata().getOrDefault("namespaces", "").split(","))
                        .contains(namespace))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("no token server for namespace: " + namespace));

        ClusterClientConfig config = new ClusterClientConfig();
        config.setServerHost(target.getIp());
        config.setServerPort(target.getPort());
        config.setRequestTimeout(20); // 默认 20ms
        ClusterClientConfigManager.registerServerAssignProperty(new DynamicSentinelProperty<>(config));

        // 当前节点标记为 Cluster Client
        ClusterStateManager.applyState(ClusterStateManager.CLUSTER_CLIENT);
    }
}
```

然后在业务规则里把 `clusterMode` 打开（或复用第 5 节的 Nacos 规则），Token Client 才会向 Token Server 申请令牌。
Token Server 不可达时，普通集群流控会按 `fallbackToLocalWhenFail`（默认 `true`）退化为本地限流。

## 7. 生产注意事项

1. **一个 namespace 只对应一个 Token Server 实例**。Sentinel 的集群令牌统计是单机内存态，同一 namespace 部署多个 Token Server 会各算各的阈值，需要多实例时请按 namespace 分片（本工程已支持一份进程承载多个 namespace）。
2. **Nacos 端口别填错**：Nacos 2.x/3.x 的 8080 是**控制台**端口，8848 才是**服务端 API/客户端连接**端口。客户端必须使用 `localhost:8848`；同时客户端还会使用 `8848 + 1000/1001` 即 9848/9849 的 gRPC 端口，容器/防火墙需要一并放通。
3. **Nacos 开启了鉴权**：必须配置 `NACOS_USERNAME` / `NACOS_PASSWORD`，否则注册会失败并报
   `Code: 401, Message: User not found! Please check user exist or password is right!`。
4. **Nacos 3.x 的 OpenAPI 变化**：v1/v2 的 `/nacos/v1/ns/...`、`/nacos/v2/ns/...` 已移除，用 curl 验证注册结果时请改用控制台（8080）或在控制台「服务管理 → 服务列表」中查看 `sentinel-token-server`。
5. **只开放了 8848 的场景**：若无法放通 9848/9849（或对接的是 Nacos 1.x 服务端），把 `pom.xml` 里的 `nacos-client.version` 改为 `1.4.8`，走 HTTP 协议。
6. **注册失败即启动失败**：`NacosTokenServerRegistry` 注册异常会直接中断启动，避免出现"服务起来了但客户端发现不到"的中间状态；如果只想手工指定地址，可设置 `NACOS_DISCOVERY_ENABLED=false`。
7. **规则可通过 Dashboard 或 Nacos 推送**，两者最终都落到 Token Server 的 `ClusterFlowRuleManager`；本工程用的是 Nacos 拉取模式，Dashboard 修改规则是否回写 Nacos 取决于 Dashboard 的二次改造情况。
8. 进程退出时（`SIGTERM`）会先摘除 Nacos 实例再关闭 Netty 服务，配合 `server.shutdown=graceful` 可减少客户端请求失败。
