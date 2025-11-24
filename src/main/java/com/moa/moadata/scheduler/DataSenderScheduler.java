package com.moa.moadata.scheduler;

import com.moa.moadata.client.MoaApiClient;
import com.moa.moadata.model.HttpPageSample;
import com.moa.moadata.reader.S3DataReader;
import com.moa.moadata.sse.service.SseEmitterService;
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

    private final S3DataReader s3DataReader;
    private final MoaApiClient moaApiClient;
    private final SseEmitterService sseEmitterService;

    @Value("${moa.data.batch-size}")
    private int batchSize;

    // 스케줄러 활성화 플래그
    private final AtomicBoolean enabled = new AtomicBoolean(false);

    @Scheduled(fixedRateString = "${moa.data.send-interval}")
    public void sendDataPeriodically() {
        // 활성화되지 않았으면 실행 안함
        if (!enabled.get()) {
            return;
        }

        if (!s3DataReader.hasNext()) {
            log.info("⏸️  전송할 데이터가 없습니다");
            return;
        }

        List<HttpPageSample> batch = s3DataReader.readNextBatch(batchSize);

        if (batch.isEmpty()) {
            log.warn("배치가 비어있습니다");
            return;
        }

        // 1️⃣ 백엔드 API로 배치 전송 (DB 저장용)
        moaApiClient.sendBatch(batch);

        // 2️⃣ SSE로 프론트엔드에 실시간 전송 ⭐ 추가!
        sseEmitterService.sendBatchData(batch);

        int current = s3DataReader.getCurrentIndex();
        int total = s3DataReader.getTotalSize();
        double progress = (double) current / total * 100;

        log.info("📊 진행 상황: {}/{} ({:.1f}%) | SSE 클라이언트: {}개",
                current, total, progress, sseEmitterService.getEmitterCount());
    }

    /**
     * 데이터 전송 시작
     */
    public void start() {
        enabled.set(true);
        log.info("🟢 데이터 전송 시작! (백엔드 API + SSE)");
    }

    /**
     * 데이터 전송 정지
     */
    public void stop() {
        enabled.set(false);
        log.info("🔴 데이터 전송 정지!");
    }

    /**
     * 데이터 전송 재시작 (인덱스 초기화)
     */
    public void restart() {
        enabled.set(false);
        enabled.set(true);
        log.info("🔄 데이터 전송 재시작!");
    }

    /**
     * 현재 상태 확인
     */
    public boolean isEnabled() {
        return enabled.get();
    }
}