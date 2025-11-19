package com.moa.moadata.sse.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.moa.moadata.model.HttpPageSample;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
@Component
@RequiredArgsConstructor
public class SseEmitterService {

    private final ObjectMapper objectMapper;

    // 연결된 클라이언트들을 저장
    private final List<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    // 타임아웃: 30분
    private static final Long TIMEOUT = 30 * 60 * 1000L;

    /**
     * SSE 연결 생성
     */
    public SseEmitter createEmitter() {
        SseEmitter emitter = new SseEmitter(TIMEOUT);

        emitters.add(emitter);
        log.info("✅ SSE 클라이언트 연결 (총 {}개)", emitters.size());

        // 연결 완료 시
        emitter.onCompletion(() -> {
            emitters.remove(emitter);
            log.info("🔌 SSE 클라이언트 연결 종료 (총 {}개)", emitters.size());
        });

        // 타임아웃 시
        emitter.onTimeout(() -> {
            emitters.remove(emitter);
            log.warn("⏰ SSE 클라이언트 타임아웃 (총 {}개)", emitters.size());
        });

        // 에러 시
        emitter.onError((e) -> {
            emitters.remove(emitter);
            log.error("❌ SSE 클라이언트 에러 (총 {}개)", emitters.size(), e);
        });

        // 초기 연결 메시지
        try {
            emitter.send(SseEmitter.event()
                    .name("connected")
                    .data(Map.of(
                            "message", "SSE 연결 성공",
                            "timestamp", System.currentTimeMillis()
                    )));
        } catch (IOException e) {
            log.error("초기 메시지 전송 실패", e);
            emitters.remove(emitter);  // ⭐ 실패하면 바로 제거
        }

        return emitter;
    }

    /**
     * 배치 데이터 전송
     */
    public void sendBatchData(List<HttpPageSample> samples) {
        if (emitters.isEmpty()) {
            log.debug("⚠️ 연결된 SSE 클라이언트가 없습니다");
            return;
        }

        log.debug("📤 SSE 배치 데이터 전송 시도: {}건, 클라이언트: {}개", samples.size(), emitters.size());

        List<SseEmitter> deadEmitters = new CopyOnWriteArrayList<>();

        for (SseEmitter emitter : emitters) {
            try {
                // ⭐ 각 emitter가 유효한지 먼저 체크
                emitter.send(SseEmitter.event()
                        .name("batch-data")
                        .data(samples, MediaType.APPLICATION_JSON));

                log.debug("✅ SSE 전송 성공");

            } catch (IllegalStateException e) {
                // ⭐ 연결이 끊어진 경우
                log.warn("⚠️ SSE 전송 실패 (연결 끊김), 클라이언트 제거");
                deadEmitters.add(emitter);

            } catch (IOException e) {
                // ⭐ IO 에러
                log.warn("⚠️ SSE 전송 실패 (IO 에러), 클라이언트 제거: {}", e.getMessage());
                deadEmitters.add(emitter);

            } catch (Exception e) {
                // ⭐ 기타 에러
                log.error("❌ SSE 전송 중 예상치 못한 에러, 클라이언트 제거", e);
                deadEmitters.add(emitter);
            }
        }

        // 실패한 emitter 제거
        emitters.removeAll(deadEmitters);

        if (!deadEmitters.isEmpty()) {
            log.info("🗑️ 죽은 연결 {}개 제거됨 (남은 연결: {}개)",
                    deadEmitters.size(), emitters.size());
        }

        // ⭐ 성공적으로 전송된 클라이언트 수 로그
        int successCount = emitters.size() - deadEmitters.size();
        if (successCount > 0) {
            log.info("✅ SSE 배치 데이터 전송 완료: {}건 → {}개 클라이언트", samples.size(), successCount);
        }
    }

    /**
     * 단건 데이터 전송
     */
    public void sendSingleData(HttpPageSample sample) {
        if (emitters.isEmpty()) {
            log.debug("⚠️ 연결된 SSE 클라이언트가 없습니다");
            return;
        }

        log.debug("📤 SSE 단건 데이터 전송, 클라이언트: {}개", emitters.size());

        List<SseEmitter> deadEmitters = new CopyOnWriteArrayList<>();

        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name("single-data")
                        .data(sample, MediaType.APPLICATION_JSON));

            } catch (IllegalStateException e) {
                log.warn("⚠️ SSE 전송 실패 (연결 끊김), 클라이언트 제거");
                deadEmitters.add(emitter);

            } catch (IOException e) {
                log.warn("⚠️ SSE 전송 실패 (IO 에러), 클라이언트 제거");
                deadEmitters.add(emitter);

            } catch (Exception e) {
                log.error("❌ SSE 전송 중 예상치 못한 에러, 클라이언트 제거", e);
                deadEmitters.add(emitter);
            }
        }

        emitters.removeAll(deadEmitters);
    }

    /**
     * 상태 메시지 전송
     */
    public void sendStatus(String message) {
        if (emitters.isEmpty()) {
            return;
        }

        log.debug("📤 SSE 상태 전송: {}, 클라이언트: {}개", message, emitters.size());

        List<SseEmitter> deadEmitters = new CopyOnWriteArrayList<>();

        for (SseEmitter emitter : emitters) {
            try {
                emitter.send(SseEmitter.event()
                        .name("status")
                        .data(Map.of(
                                "message", message,
                                "timestamp", System.currentTimeMillis()
                        )));

            } catch (Exception e) {
                deadEmitters.add(emitter);
            }
        }

        emitters.removeAll(deadEmitters);
    }

    /**
     * 연결된 클라이언트 수
     */
    public int getEmitterCount() {
        return emitters.size();
    }
}