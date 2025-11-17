package com.moa.moadata.client;

import com.moa.moadata.model.HttpPageSample;
import com.moa.moadata.websocket.WebSocketDataPublisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.List;

@Slf4j
@Component
public class MoaApiClient {

    private final RestTemplate restTemplate;
    private final String moaBackendUrl;
    private final WebSocketDataPublisher webSocketPublisher;

    public MoaApiClient(
            RestTemplate restTemplate,
            @Value("${moa.backend.url}") String moaBackendUrl,
            WebSocketDataPublisher webSocketPublisher) {
        this.restTemplate = restTemplate;
        this.moaBackendUrl = moaBackendUrl;
        this.webSocketPublisher = webSocketPublisher;
    }

    /**
     * 배치로 데이터 전송
     * 1. 백엔드 API로 전송 (DB 저장용)
     * 2. 웹소켓으로 전송 (프론트엔드 실시간 업데이트용)
     */
    public void sendBatch(List<HttpPageSample> samples) {
        if (samples == null || samples.isEmpty()) {
            log.warn("전송할 데이터가 없습니다");
            return;
        }

        // 🔹 1. 백엔드 API로 전송 (DB 저장용)
        sendToBackend(samples);

        // 🔹 2. 웹소켓으로 전송 (프론트엔드 실시간 업데이트용)
        // → 같은 객체를 전송하므로 ts_server 값이 동일함!
        webSocketPublisher.publishBatchData(samples);
    }

    /**
     * 단건 데이터 전송
     * 1. 백엔드 API로 전송 (DB 저장용)
     * 2. 웹소켓으로 전송 (프론트엔드 실시간 업데이트용)
     */
    public void send(HttpPageSample sample) {
        if (sample == null) {
            log.warn("전송할 데이터가 없습니다");
            return;
        }

        // 🔹 1. 백엔드 API로 전송 (DB 저장용)
        sendToBackendSingle(sample);

        // 🔹 2. 웹소켓으로 전송 (프론트엔드 실시간 업데이트용)
        webSocketPublisher.publishSingleData(sample);
    }

    /**
     * 백엔드 API로 배치 전송 (내부 메서드)
     */
    private void sendToBackend(List<HttpPageSample> samples) {
        try {
            String url = moaBackendUrl + "/page-samples/batch";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<List<HttpPageSample>> request = new HttpEntity<>(samples, headers);

            ResponseEntity<Void> response = restTemplate.postForEntity(url, request, Void.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("✅ 백엔드 배치 전송 성공: {}개", samples.size());
            } else {
                log.error("❌ 백엔드 배치 전송 실패: status={}", response.getStatusCode());
            }

        } catch (Exception e) {
            log.error("❌ 백엔드 배치 전송 중 오류 발생", e);
        }
    }

    /**
     * 백엔드 API로 단건 전송 (내부 메서드)
     */
    private void sendToBackendSingle(HttpPageSample sample) {
        try {
            String url = moaBackendUrl + "/page-samples";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<HttpPageSample> request = new HttpEntity<>(sample, headers);

            ResponseEntity<Void> response = restTemplate.postForEntity(url, request, Void.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                log.debug("✅ 백엔드 데이터 전송 성공");
            } else {
                log.error("❌ 백엔드 데이터 전송 실패: status={}", response.getStatusCode());
            }

        } catch (Exception e) {
            log.error("❌ 백엔드 데이터 전송 중 오류 발생", e);
        }
    }
}