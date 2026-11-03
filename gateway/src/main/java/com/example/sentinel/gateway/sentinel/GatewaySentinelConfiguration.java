package com.example.sentinel.gateway.sentinel;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.csp.sentinel.adapter.gateway.sc.SentinelGatewayFilter;
import com.alibaba.csp.sentinel.adapter.gateway.sc.callback.GatewayCallbackManager;
import com.alibaba.csp.sentinel.adapter.gateway.sc.exception.SentinelGatewayBlockExceptionHandler;
import com.alibaba.csp.sentinel.slots.block.BlockException;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.route.Route;
import org.springframework.cloud.gateway.support.ServerWebExchangeUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.reactive.result.view.ViewResolver;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * Sentinel 在 Spring Cloud Gateway 上的装配。
 *
 * <p>资源名使用路由 id（RouteResourceMode.ROUTE_ID），因此 Dashboard / Nacos 中的
 * 规则的 resource 直接填路由 id 即可，例如 order-api。</p>
 */
@Configuration
public class GatewaySentinelConfiguration {

    private static final Logger log = LoggerFactory.getLogger(GatewaySentinelConfiguration.class);

    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public SentinelGatewayBlockExceptionHandler sentinelGatewayBlockExceptionHandler(
            List<ViewResolver> viewResolvers, ServerCodecConfigurer serverCodecConfigurer) {
        return new SentinelGatewayBlockExceptionHandler(viewResolvers, serverCodecConfigurer);
    }

    @Bean
    public SentinelGatewayFilter sentinelGatewayFilter() {
        return new SentinelGatewayFilter();
    }

    @PostConstruct
    public void initBlockHandler() {
        GatewayCallbackManager.setBlockHandler(GatewaySentinelConfiguration::buildBlockedResponse);
        log.info("[Gateway] Sentinel block handler initialized, resources are route ids");
    }

    /**
     * 统一返回 429 JSON，便于前端/调用方区分限流与后端错误。
     */
    private static Mono<ServerResponse> buildBlockedResponse(ServerWebExchange exchange, Throwable throwable) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", HttpStatus.TOO_MANY_REQUESTS.value());
        body.put("message", "Too many requests, blocked by Sentinel");
        body.put("path", exchange.getRequest().getPath().value());
        body.put("route", resolveRouteId(exchange));
        if (throwable instanceof BlockException blockException) {
            body.put("resource", blockException.getRule() == null ? null : blockException.getRule().getResource());
            body.put("ruleLimitApp", blockException.getRuleLimitApp());
        }
        log.warn("[Gateway] Request blocked, path={}, route={}, reason={}",
                body.get("path"), body.get("route"), throwable == null ? null : throwable.getClass().getSimpleName());
        return ServerResponse.status(HttpStatus.TOO_MANY_REQUESTS)
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(body));
    }

    private static String resolveRouteId(ServerWebExchange exchange) {
        Route route = exchange.getAttribute(ServerWebExchangeUtils.GATEWAY_ROUTE_ATTR);
        return route == null ? null : route.getId();
    }
}
