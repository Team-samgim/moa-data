package com.moa.moadata.scheduler;

import com.moa.moadata.client.MoaApiClient;
import com.moa.moadata.model.HttpPageSample;
import com.moa.moadata.reader.ExcelDataReader;
import com.moa.moadata.websocket.WebSocketDataPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@Component
@RequiredArgsConstructor
public class DataSenderScheduler {

    private final ExcelDataReader excelDataReader;
    private final MoaApiClient moaApiClient;
    private final WebSocketDataPublisher webSocketPublisher;

    @Value("${moa.data.batch-size}")
    private int batchSize;

    // 스케줄러 활성화 플래그
    private final AtomicBoolean enabled = new AtomicBoolean(false);  // 기본값: false (꺼짐)

    @Scheduled(fixedRateString = "${moa.data.send-interval}")
    public void sendDataPeriodically() {
        // 활성화되지 않았으면 실행 안함
        if (!enabled.get()) {
            return;
        }

        if (!excelDataReader.hasNext()) {
            log.info("⏸️  전송할 데이터가 없습니다");
            return;
        }

        List<HttpPageSample> batch = excelDataReader.readNextBatch(batchSize);

        if (batch.isEmpty()) {
            log.warn("배치가 비어있습니다");
            return;
        }

        // 🔹 백엔드 API + 웹소켓 동시 전송
        // MoaApiClient 내부에서 백엔드와 웹소켓 모두 처리
        moaApiClient.sendBatch(batch);

        int current = excelDataReader.getCurrentIndex();
        int total = excelDataReader.getTotalSize();
        double progress = (double) current / total * 100;

        log.info("📊 진행 상황: {}/{} ({:.1f}%)", current, total, progress);
    }

    /**
     * 데이터 전송 시작
     */
    public void start() {
        enabled.set(true);
        // 웹소켓으로 시작 상태 알림
        webSocketPublisher.publishConnectionStatus("started", "🟢 실시간 데이터 전송 시작");
        log.info("🟢 데이터 전송 시작! (백엔드 API + 웹소켓)");
    }

    /**
     * 데이터 전송 정지
     */
    public void stop() {
        enabled.set(false);
        // 웹소켓으로 정지 상태 알림
        webSocketPublisher.publishConnectionStatus("stopped", "🔴 실시간 데이터 전송 정지");
        log.info("🔴 데이터 전송 정지!");
    }

    /**
     * 데이터 전송 재시작 (인덱스 초기화)
     */
    public void restart() {
        enabled.set(false);
        // 엑셀 리더 초기화 로직 필요하면 추가
        enabled.set(true);
        webSocketPublisher.publishConnectionStatus("restarted", "🔄 실시간 데이터 전송 재시작");
        log.info("🔄 데이터 전송 재시작!");
    }

    /**
     * 현재 상태 확인
     */
    public boolean isEnabled() {
        return enabled.get();
    }
}