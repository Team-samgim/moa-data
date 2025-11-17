package com.moa.moadata.websocket;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.messaging.simp.annotation.SubscribeMapping;
import org.springframework.stereotype.Controller;

import java.util.HashMap;
import java.util.Map;

/**
 * 웹소켓 메시지 핸들링 컨트롤러
 */
@Slf4j
@Controller
@RequiredArgsConstructor
public class WebSocketController {

    /**
     * 클라이언트가 /topic/status를 구독할 때 호출
     * 현재 연결 상태를 즉시 전송
     */
    @SubscribeMapping("/status")
    public Map<String, Object> onSubscribeStatus() {
        log.info("🌐 클라이언트가 상태 채널에 구독함");

        Map<String, Object> status = new HashMap<>();
        status.put("status", "connected");
        status.put("message", "웹소켓 연결 완료");
        status.put("timestamp", System.currentTimeMillis());

        return status;
    }

    /**
     * 클라이언트가 /topic/page-samples/batch를 구독할 때 호출
     */
    @SubscribeMapping("/page-samples/batch")
    public Map<String, Object> onSubscribeBatch() {
        log.info("🌐 클라이언트가 배치 데이터 채널에 구독함");

        Map<String, Object> response = new HashMap<>();
        response.put("status", "subscribed");
        response.put("message", "배치 데이터 구독 완료");
        response.put("timestamp", System.currentTimeMillis());

        return response;
    }

    /**
     * 클라이언트에서 Ping 메시지를 보낼 때
     * 사용법: stompClient.send("/app/ping", {})
     */
    @MessageMapping("/ping")
    @SendTo("/topic/pong")
    public Map<String, Object> handlePing() {
        Map<String, Object> pong = new HashMap<>();
        pong.put("message", "pong");
        pong.put("timestamp", System.currentTimeMillis());
        return pong;
    }
}