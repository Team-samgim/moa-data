package com.moa.moadata.websocket;

import com.moa.moadata.model.HttpPageSample;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 웹소켓을 통해 프론트엔드로 실시간 데이터를 전송하는 서비스
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WebSocketDataPublisher {

    private final SimpMessagingTemplate messagingTemplate;

    /**
     * 단건 데이터를 프론트엔드로 전송
     * 토픽: /topic/page-samples
     */
    public void publishSingleData(HttpPageSample sample) {
        try {
            messagingTemplate.convertAndSend("/topic/page-samples", sample);
            log.debug("🌐 웹소켓 단건 데이터 전송 완료");
        } catch (Exception e) {
            log.error("❌ 웹소켓 단건 데이터 전송 실패", e);
        }
    }

    /**
     * 배치 데이터를 프론트엔드로 전송
     * 토픽: /topic/page-samples/batch
     */
    public void publishBatchData(List<HttpPageSample> samples) {
        if (samples == null || samples.isEmpty()) {
            log.warn("전송할 웹소켓 데이터가 없습니다");
            return;
        }

        try {
            messagingTemplate.convertAndSend("/topic/page-samples/batch", samples);
            log.info("🌐 웹소켓 배치 데이터 전송 완료: {}건", samples.size());
        } catch (Exception e) {
            log.error("❌ 웹소켓 배치 데이터 전송 실패", e);
        }
    }

    /**
     * 연결 상태 알림
     * 토픽: /topic/status
     */
    public void publishConnectionStatus(String status, String message) {
        try {
            Map<String, Object> statusMsg = new HashMap<>();
            statusMsg.put("status", status);
            statusMsg.put("message", message);
            statusMsg.put("timestamp", System.currentTimeMillis());

            messagingTemplate.convertAndSend("/topic/status", statusMsg);
            log.info("🌐 연결 상태 전송: {}", status);
        } catch (Exception e) {
            log.error("❌ 연결 상태 전송 실패", e);
        }
    }
}