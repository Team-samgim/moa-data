package com.moa.moadata.sse.controller;

import com.moa.moadata.sse.service.SseEmitterService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * SSE 엔드포인트
 */
@Slf4j
@RestController
@RequestMapping("/api/sse")
@RequiredArgsConstructor
@CrossOrigin(origins = "*")  // CORS 허용
public class SseController {

    private final SseEmitterService sseEmitterService;

    /**
     * SSE 연결
     * GET /api/sse/connect
     */
    @GetMapping(value = "/connect", produces = "text/event-stream;charset=UTF-8")
    public SseEmitter connect() {
        log.info("🔗 SSE 연결 요청");
        return sseEmitterService.createEmitter();
    }
}