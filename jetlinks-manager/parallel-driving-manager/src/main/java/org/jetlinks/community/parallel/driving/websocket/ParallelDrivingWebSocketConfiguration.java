package org.jetlinks.community.parallel.driving.websocket;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;

import java.util.HashMap;
import java.util.Map;

/**
 * 平行驾驶 WebSocket 配置
 *
 * @author JetLinks
 */
@Configuration
public class ParallelDrivingWebSocketConfiguration {
    
    private final ParallelDrivingWebSocketHandler webSocketHandler;
    
    public ParallelDrivingWebSocketConfiguration(ParallelDrivingWebSocketHandler webSocketHandler) {
        this.webSocketHandler = webSocketHandler;
    }
    
    /**
     * 注册 WebSocket 处理器
     */
    @Bean
    public HandlerMapping parallelDrivingWebSocketHandlerMapping() {
        Map<String, WebSocketHandler> map = new HashMap<>();
        map.put("/parallel-driving/ws", webSocketHandler);
        
        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setUrlMap(map);
        mapping.setOrder(-1); // 设置优先级
        return mapping;
    }
    
    /**
     * WebSocket 处理器适配器
     */
    @Bean
    public WebSocketHandlerAdapter webSocketHandlerAdapter() {
        return new WebSocketHandlerAdapter();
    }
}
