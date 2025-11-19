package com.moa.moadata.client;

import com.moa.moadata.model.HttpPageSample;
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

    public MoaApiClient(
            RestTemplate restTemplate,
            @Value("${moa.backend.url}") String moaBackendUrl) {
        this.restTemplate = restTemplate;
        this.moaBackendUrl = moaBackendUrl;
    }

    /**
     * 배치로 데이터 전송
     */
    public void sendBatch(List<HttpPageSample> samples) {
        if (samples == null || samples.isEmpty()) {
            log.warn("전송할 데이터가 없습니다");
            return;
        }

        try {
            String url = moaBackendUrl + "/page-samples/batch";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<List<HttpPageSample>> request = new HttpEntity<>(samples, headers);

            ResponseEntity<Void> response = restTemplate.postForEntity(url, request, Void.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                log.info("✅ 배치 전송 성공: {}개", samples.size());
            } else {
                log.error("❌ 배치 전송 실패: status={}", response.getStatusCode());
            }

        } catch (Exception e) {
            log.error("❌ 배치 전송 중 오류 발생", e);
        }
    }

    /**
     * 단건 데이터 전송
     */
    public void send(HttpPageSample sample) {
        if (sample == null) {
            log.warn("전송할 데이터가 없습니다");
            return;
        }

        try {
            String url = moaBackendUrl + "/page-samples";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<HttpPageSample> request = new HttpEntity<>(sample, headers);

            ResponseEntity<Void> response = restTemplate.postForEntity(url, request, Void.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                log.debug("✅ 데이터 전송 성공");
            } else {
                log.error("❌ 데이터 전송 실패: status={}", response.getStatusCode());
            }

        } catch (Exception e) {
            log.error("❌ 데이터 전송 중 오류 발생", e);
        }
    }
}