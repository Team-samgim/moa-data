package com.moa.moadata.reader;

import com.moa.moadata.model.HttpPageSample;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

@Slf4j
@SpringBootTest
class ExcelDataReaderTest {

    @Autowired
    private ExcelDataReader excelDataReader;

    @Test
    void testReadExcelData() {
        log.info("========== 엑셀 데이터 읽기 테스트 시작 ==========");

        // 첫 번째 배치 읽기
        List<HttpPageSample> batch = excelDataReader.readNextBatch(5);

        log.info("✅ 읽은 데이터 개수: {}", batch.size());
        log.info("==============================================");

        // 각 데이터 출력
        for (int i = 0; i < batch.size(); i++) {
            HttpPageSample sample = batch.get(i);
            log.info("\n📦 데이터 #{}", i + 1);
            log.info("  row_key: {}", sample.getRowKey());
            log.info("  src_ip: {}", sample.getSrcIp());
            log.info("  dst_ip: {}", sample.getDstIp());
            log.info("  src_mac: {}", sample.getSrcMac());
            log.info("  dst_mac: {}", sample.getDstMac());
            log.info("  src_port: {}", sample.getSrcPort());
            log.info("  dst_port: {}", sample.getDstPort());
            log.info("  page_http_len: {}", sample.getPageHttpLen());
            log.info("  page_http_len_req: {}", sample.getPageHttpLenReq());
            log.info("  page_http_len_res: {}", sample.getPageHttpLenRes());
            log.info("  http_method: {}", sample.getHttpMethod());
            log.info("  http_host: {}", sample.getHttpHost());
            log.info("  http_uri: {}", sample.getHttpUri());
            log.info("  http_res_code: {}", sample.getHttpResCode());
            log.info("  mbps: {}", sample.getMbps());
            log.info("  pps: {}", sample.getPps());
            log.info("  country_name_req: {}", sample.getCountryNameReq());
            log.info("  user_agent_software_name: {}", sample.getUserAgentSoftwareName());
            log.info("  ts_page: {}", sample.getTsPage());
        }

        log.info("\n========== 테스트 완료 ==========");
    }
}