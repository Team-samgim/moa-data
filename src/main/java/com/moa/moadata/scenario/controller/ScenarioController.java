package com.moa.moadata.scenario.controller;

import com.moa.moadata.scenario.service.ScenarioService;
import com.moa.moadata.scheduler.DataSenderScheduler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/scenario")
@RequiredArgsConstructor
public class ScenarioController {

    private final ScenarioService scenarioService;
    private final DataSenderScheduler scheduler;  // ← 추가

    // ========== 데이터 전송 제어 ==========

    /**
     * 데이터 전송 시작
     * POST /scenario/start
     */
    @PostMapping("/start")
    public Map<String, Object> start() {
        scheduler.start();

        Map<String, Object> response = new HashMap<>();
        response.put("status", "started");
        response.put("message", "🟢 실시간 데이터 전송 시작");
        return response;
    }

    /**
     * 데이터 전송 정지
     * POST /scenario/stop
     */
    @PostMapping("/stop")
    public Map<String, Object> stop() {
        scheduler.stop();

        Map<String, Object> response = new HashMap<>();
        response.put("status", "stopped");
        response.put("message", "🔴 실시간 데이터 전송 정지");
        return response;
    }

    /**
     * 현재 상태 확인
     * GET /scenario/status
     */
    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> response = new HashMap<>();
        response.put("enabled", scheduler.isEnabled());
        response.put("status", scheduler.isEnabled() ? "running" : "stopped");
        response.put("message", scheduler.isEnabled() ? "데이터 전송 중" : "대기 중");
        return response;
    }

    // ========== 시나리오 실행 (기존 코드) ==========

    /**
     * 특정 국가 느려짐
     * POST /scenario/slow-country?country=KR&count=20
     */
    @PostMapping("/slow-country")
    public String slowCountry(
            @RequestParam(defaultValue = "KR") String country,
            @RequestParam(defaultValue = "20") int count) {

        scenarioService.triggerSlowCountry(country, count);
        return "✅ " + country + " 국가 느려짐 시나리오 실행 완료";
    }

    /**
     * HTTP 5xx 에러 급증
     * POST /scenario/error-spike?count=30
     */
    @PostMapping("/error-spike")
    public String errorSpike(@RequestParam(defaultValue = "30") int count) {
        scenarioService.triggerErrorSpike(count);
        return "✅ 5xx 에러 급증 시나리오 실행 완료";
    }

    /**
     * TCP 에러 발생
     * POST /scenario/tcp-error?count=25
     */
    @PostMapping("/tcp-error")
    public String tcpError(@RequestParam(defaultValue = "25") int count) {
        scenarioService.triggerTcpError(count);
        return "✅ TCP 에러 시나리오 실행 완료";
    }

    /**
     * 특정 브라우저 문제
     * POST /scenario/browser-issue?browser=Firefox&count=15
     */
    @PostMapping("/browser-issue")
    public String browserIssue(
            @RequestParam(defaultValue = "Firefox") String browser,
            @RequestParam(defaultValue = "15") int count) {

        scenarioService.triggerBrowserIssue(browser, count);
        return "✅ " + browser + " 브라우저 문제 시나리오 실행 완료";
    }

    /**
     * 정상 복구
     * POST /scenario/recover?count=50
     */
    @PostMapping("/recover")
    public String recover(@RequestParam(defaultValue = "50") int count) {
        scenarioService.recover(count);
        return "✅ 정상 복구 완료";
    }
}