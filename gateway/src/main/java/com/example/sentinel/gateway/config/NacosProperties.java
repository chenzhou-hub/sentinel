package com.example.sentinel.gateway.config;

import java.util.Properties;

import com.alibaba.nacos.api.PropertyKeyConst;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.StringUtils;

/**
 * Nacos 连接配置，对应 application.yml 中 nacos.* 节点。
 */
@ConfigurationProperties(prefix = "nacos")
public class NacosProperties {

    /**
     * Nacos 服务端地址，集群多地址用逗号分隔。
     * 注意：Nacos 2.x 的 8080 是控制台端口，客户端应连服务端口 8848。
     */
    private String serverAddr = "localhost:8848";

    /**
     * 命名空间 ID，留空表示 public。
     */
    private String namespace;

    private String username;

    private String password;

    /**
     * 转换为 nacos-client 需要的 {@link Properties}。
     */
    public Properties toProperties() {
        Properties properties = new Properties();
        properties.setProperty(PropertyKeyConst.SERVER_ADDR, serverAddr);
        if (StringUtils.hasText(namespace)) {
            properties.setProperty(PropertyKeyConst.NAMESPACE, namespace);
        }
        if (StringUtils.hasText(username)) {
            properties.setProperty(PropertyKeyConst.USERNAME, username);
        }
        if (StringUtils.hasText(password)) {
            properties.setProperty(PropertyKeyConst.PASSWORD, password);
        }
        return properties;
    }

    public String getServerAddr() {
        return serverAddr;
    }

    public void setServerAddr(String serverAddr) {
        this.serverAddr = serverAddr;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
