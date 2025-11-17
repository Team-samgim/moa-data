package com.moa.moadata.scenario.service;

import com.moa.moadata.client.MoaApiClient;
import com.moa.moadata.model.HttpPageSample;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ScenarioService {

    private final MoaApiClient moaApiClient;

    /**
     * 시나리오 1: 특정 국가 느려짐
     */
    public void triggerSlowCountry(String country, int count) {
        log.warn("🚨 시나리오 시작: {} 국가 느려짐 ({}건)", country, count);

        List<HttpPageSample> badSamples = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            HttpPageSample sample = createBadSample();
            sample.setCountryNameReq(country);
            sample.setTsPage(15000.0 + (Math.random() * 5000)); // 15~20초
            sample.setTsPageRes(10000.0 + (Math.random() * 3000)); // 10~13초
            badSamples.add(sample);
        }

        moaApiClient.sendBatch(badSamples);
        log.info("✅ {} 국가 느려짐 데이터 {}건 전송 완료", country, count);
    }

    /**
     * 시나리오 2: HTTP 5xx 에러 급증
     */
    public void triggerErrorSpike(int count) {
        log.warn("🚨 시나리오 시작: 5xx 에러 급증 ({}건)", count);

        List<HttpPageSample> errorSamples = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            HttpPageSample sample = createBadSample();
            sample.setHttpResCode("500");
            sample.setResCode5xxCnt(1);
            sample.setTsPage(8000.0 + (Math.random() * 2000)); // 8~10초
            errorSamples.add(sample);
        }

        moaApiClient.sendBatch(errorSamples);
        log.info("✅ 5xx 에러 데이터 {}건 전송 완료", count);
    }

    /**
     * 시나리오 3: TCP 에러 발생
     */
    public void triggerTcpError(int count) {
        log.warn("🚨 시나리오 시작: TCP 에러 발생 ({}건)", count);

        List<HttpPageSample> tcpErrorSamples = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            HttpPageSample sample = createBadSample();
            sample.setTcpErrorCnt(5 + (int)(Math.random() * 10)); // 5~15개
            sample.setRetransmissionCnt(3 + (int)(Math.random() * 5)); // 3~8개
            sample.setTsPage(6000.0 + (Math.random() * 2000)); // 6~8초
            tcpErrorSamples.add(sample);
        }

        moaApiClient.sendBatch(tcpErrorSamples);
        log.info("✅ TCP 에러 데이터 {}건 전송 완료", count);
    }

    /**
     * 시나리오 4: 특정 브라우저 문제
     */
    public void triggerBrowserIssue(String browser, int count) {
        log.warn("🚨 시나리오 시작: {} 브라우저 문제 ({}건)", browser, count);

        List<HttpPageSample> browserSamples = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            HttpPageSample sample = createBadSample();
            sample.setUserAgentSoftwareName(browser);
            sample.setTsPage(12000.0 + (Math.random() * 3000)); // 12~15초
            sample.setHttpResCode("408"); // Timeout
            browserSamples.add(sample);
        }

        moaApiClient.sendBatch(browserSamples);
        log.info("✅ {} 브라우저 문제 데이터 {}건 전송 완료", browser, count);
    }

    /**
     * 정상 복구
     */
    public void recover(int count) {
        log.info("✅ 시나리오 종료: 정상 복구 ({}건)", count);

        List<HttpPageSample> normalSamples = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            HttpPageSample sample = createNormalSample();
            normalSamples.add(sample);
        }

        moaApiClient.sendBatch(normalSamples);
        log.info("✅ 정상 데이터 {}건 전송 완료", count);
    }

    // Helper: 나쁜 샘플 생성
    private HttpPageSample createBadSample() {
        return HttpPageSample.builder()
                .rowKey(UUID.randomUUID().toString())
                .srcIp("192.168.1." + (int)(Math.random() * 255))
                .dstIp("10.0.0." + (int)(Math.random() * 255))
                .srcPort(50000 + (int)(Math.random() * 10000))
                .dstPort(80)
                .tsServer(LocalDateTime.now())
                .countryNameReq("KR")
                .userAgentSoftwareName("Chrome")
                .userAgentHardwareType("Desktop")
                .httpMethod("GET")
                .httpHost("example.com")
                .httpUri("/api/slow")
                .httpResCode("200")
                .pageHttpCntReq(1)
                .pageHttpCntRes(1)
                .createdAt(LocalDateTime.now())
                .build();
    }

    // Helper: 정상 샘플 생성
    private HttpPageSample createNormalSample() {
        HttpPageSample sample = createBadSample();
        sample.setTsPage(1000.0 + (Math.random() * 1000)); // 1~2초
        sample.setTsPageRes(500.0 + (Math.random() * 500)); // 0.5~1초
        sample.setHttpResCode("200");
        sample.setTcpErrorCnt(0);
        return sample;
    }
}