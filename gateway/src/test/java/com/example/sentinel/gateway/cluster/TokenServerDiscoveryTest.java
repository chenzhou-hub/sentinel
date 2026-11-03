package com.example.sentinel.gateway.cluster;

import java.util.List;
import java.util.Map;

import com.alibaba.nacos.api.naming.pojo.Instance;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TokenServerDiscoveryTest {

    private static Instance instance(String ip, int port, double weight, String namespaces) {
        Instance instance = new Instance();
        instance.setIp(ip);
        instance.setPort(port);
        instance.setWeight(weight);
        instance.setHealthy(true);
        instance.setMetadata(Map.of("namespaces", namespaces));
        return instance;
    }

    @Test
    void filtersInstancesThatDoNotServeTheNamespace() {
        List<Instance> candidates = List.of(
                instance("10.0.0.2", 18730, 1.0, "demo-app"),
                instance("10.0.0.3", 18730, 1.0, "api-gateway,demo-app"));

        List<Instance> selected = TokenServerDiscovery.selectInstances(candidates, "api-gateway");

        assertThat(selected).extracting(Instance::getIp).containsExactly("10.0.0.3");
    }

    @Test
    void prefersHighestWeight() {
        List<Instance> candidates = List.of(
                instance("10.0.0.2", 18730, 1.0, "api-gateway"),
                instance("10.0.0.9", 18730, 5.0, "api-gateway"));

        List<Instance> selected = TokenServerDiscovery.selectInstances(candidates, "api-gateway");

        assertThat(selected).extracting(Instance::getIp).startsWith("10.0.0.9");
    }

    @Test
    void tieBreaksByIpSoAllClientsPickTheSameInstance() {
        List<Instance> candidates = List.of(
                instance("10.0.0.9", 18730, 1.0, "api-gateway"),
                instance("10.0.0.2", 18730, 1.0, "api-gateway"),
                instance("10.0.0.5", 18731, 1.0, "api-gateway"));

        List<Instance> selected = TokenServerDiscovery.selectInstances(candidates, "api-gateway");

        assertThat(selected).extracting(Instance::getIp).containsExactly("10.0.0.2", "10.0.0.5", "10.0.0.9");
    }

    @Test
    void ignoresUnhealthyAndEmptyInput() {
        Instance unhealthy = instance("10.0.0.2", 18730, 1.0, "api-gateway");
        unhealthy.setHealthy(false);

        assertThat(TokenServerDiscovery.selectInstances(List.of(unhealthy), "api-gateway")).isEmpty();
        assertThat(TokenServerDiscovery.selectInstances(null, "api-gateway")).isEmpty();
    }
}
