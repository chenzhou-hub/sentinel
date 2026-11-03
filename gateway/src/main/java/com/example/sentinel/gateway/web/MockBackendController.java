package com.example.sentinel.gateway.web;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * 演示用后端：路由 /api/order/** 会 forward 到这里，
 * 接入真实服务时把路由 uri 换成 lb:// 或 http:// 即可，本控制器可以删掉。
 */
@RestController
public class MockBackendController {

    @GetMapping("/mock/order")
    public Mono<Map<String, Object>> order(ServerWebExchange exchange) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("service", "mock-order");
        body.put("query", exchange.getRequest().getQueryParams().toSingleValueMap());
        body.put("timestamp", Instant.now().toString());
        return Mono.just(body);
    }
}
