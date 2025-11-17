package com.moa.moadata.reader;

import com.moa.moadata.model.HttpPageSample;
import lombok.extern.slf4j.Slf4j;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.*;

@Slf4j
@Component
public class ExcelDataReader {

    @Value("${moa.data.excel-file}")
    private Resource excelFile;

    @Value("${moa.data.loop-mode}")
    private boolean loopMode;

    private List<HttpPageSample> allData;
    private int currentIndex = 0;
    private Map<String, Integer> headerIndexMap;

    @PostConstruct
    public void init() {
        try {
            this.allData = readExcelFile();
            log.info("엑셀 파일 로딩 완료: 총 {}개 데이터", allData.size());
        } catch (IOException e) {
            log.error("엑셀 파일 로딩 실패", e);
            this.allData = new ArrayList<>();
        }
    }

    private List<HttpPageSample> readExcelFile() throws IOException {
        List<HttpPageSample> samples = new ArrayList<>();

        try (InputStream inputStream = excelFile.getInputStream();
             Workbook workbook = new XSSFWorkbook(inputStream)) {

            Sheet sheet = workbook.getSheetAt(0);

            // 1️⃣ 첫 번째 행에서 컬럼 인덱스 매핑 생성
            Row headerRow = sheet.getRow(0);
            this.headerIndexMap = buildHeaderIndexMap(headerRow);

            log.info("📋 헤더 매핑 완료: {}", headerIndexMap.keySet());

            // 2️⃣ 두 번째 행부터 데이터 읽기
            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                if (row == null) continue;

                HttpPageSample sample = parseRow(row);
                if (sample != null) {
                    samples.add(sample);
                }
            }
        }

        return samples;
    }

    // 헤더 이름 → 컬럼 인덱스 매핑
    private Map<String, Integer> buildHeaderIndexMap(Row headerRow) {
        Map<String, Integer> map = new HashMap<>();

        for (int i = 0; i < headerRow.getLastCellNum(); i++) {
            Cell cell = headerRow.getCell(i);
            if (cell != null) {
                String headerName = cell.getStringCellValue().trim();
                map.put(headerName, i);
            }
        }

        return map;
    }

    private HttpPageSample parseRow(Row row) {
        try {
            return HttpPageSample.builder()
                    // ===== 기본 키 & IP/포트 =====
                    .rowKey(getCellValueByHeader(row, "row_key"))
                    .srcIp(getCellValueByHeader(row, "src_ip"))
                    .dstIp(getCellValueByHeader(row, "dst_ip"))
                    .srcPort(getCellValueAsIntegerByHeader(row, "src_port"))
                    .dstPort(getCellValueAsIntegerByHeader(row, "dst_port"))

                    // ===== 프레임/페이지 타이밍 =====
                    .tsFrameArrival(getCellValueAsDoubleByHeader(row, "ts_frame_arrival"))
                    .tsFrameLandoff(getCellValueAsDoubleByHeader(row, "ts_frame_landoff"))
                    .pageIdx(getCellValueAsLongByHeader(row, "page_idx"))
                    .tsServerNsec(getCellValueAsDoubleByHeader(row, "ts_server_nsec"))

                    // ===== MAC =====
                    .srcMac(getCellValueByHeader(row, "src_mac"))
                    .dstMac(getCellValueByHeader(row, "dst_mac"))

                    // ===== HTTP Length =====
                    .pageHttpLen(getCellValueAsLongByHeader(row, "page_http_len"))
                    .pageHttpLenReq(getCellValueAsLongByHeader(row, "page_http_len_req"))
                    .pageHttpLenRes(getCellValueAsLongByHeader(row, "page_http_len_res"))
                    .pageHttpHeaderLenReq(getCellValueAsLongByHeader(row, "page_http_header_len_req"))
                    .pageHttpHeaderLenRes(getCellValueAsLongByHeader(row, "page_http_header_len_res"))
                    .pageHttpContentLenReq(getCellValueAsLongByHeader(row, "page_http_content_len_req"))
                    .pageHttpContentLenRes(getCellValueAsLongByHeader(row, "page_http_content_len_res"))

                    // ===== Packet Length =====
                    .pagePktLen(getCellValueAsLongByHeader(row, "page_pkt_len"))
                    .pagePktLenReq(getCellValueAsLongByHeader(row, "page_pkt_len_req"))
                    .pagePktLenRes(getCellValueAsLongByHeader(row, "page_pkt_len_res"))

                    // ===== TCP Length =====
                    .pageTcpLen(getCellValueAsLongByHeader(row, "page_tcp_len"))
                    .pageTcpLenReq(getCellValueAsLongByHeader(row, "page_tcp_len_req"))
                    .pageTcpLenRes(getCellValueAsLongByHeader(row, "page_tcp_len_res"))
                    .httpContentLength(getCellValueAsLongByHeader(row, "http_content_length"))
                    .httpContentLengthReq(getCellValueAsLongByHeader(row, "http_content_length_req"))

                    // ===== Conn / Error Length =====
                    .connErrSessionLen(getCellValueAsLongByHeader(row, "conn_err_session_len"))
                    .reqConnErrSessionLen(getCellValueAsLongByHeader(row, "req_conn_err_session_len"))
                    .resConnErrSessionLen(getCellValueAsLongByHeader(row, "res_conn_err_session_len"))
                    .retransmissionLen(getCellValueAsLongByHeader(row, "retransmission_len"))
                    .retransmissionLenReq(getCellValueAsLongByHeader(row, "retransmission_len_req"))
                    .retransmissionLenRes(getCellValueAsLongByHeader(row, "retransmission_len_res"))
                    .outOfOrderLen(getCellValueAsLongByHeader(row, "out_of_order_len"))
                    .outOfOrderLenReq(getCellValueAsLongByHeader(row, "out_of_order_len_req"))
                    .outOfOrderLenRes(getCellValueAsLongByHeader(row, "out_of_order_len_res"))
                    .lostSegLen(getCellValueAsLongByHeader(row, "lost_seg_len"))
                    .lostSegLenReq(getCellValueAsLongByHeader(row, "lost_seg_len_req"))
                    .lostSegLenRes(getCellValueAsLongByHeader(row, "lost_seg_len_res"))
                    .ackLostLen(getCellValueAsLongByHeader(row, "ack_lost_len"))
                    .ackLostLenReq(getCellValueAsLongByHeader(row, "ack_lost_len_req"))
                    .ackLostLenRes(getCellValueAsLongByHeader(row, "ack_lost_len_res"))
                    .winUpdateLen(getCellValueAsLongByHeader(row, "win_update_len"))
                    .winUpdateLenReq(getCellValueAsLongByHeader(row, "win_update_len_req"))
                    .winUpdateLenRes(getCellValueAsLongByHeader(row, "win_update_len_res"))
                    .dupAckLen(getCellValueAsLongByHeader(row, "dup_ack_len"))
                    .dupAckLenReq(getCellValueAsLongByHeader(row, "dup_ack_len_req"))
                    .dupAckLenRes(getCellValueAsLongByHeader(row, "dup_ack_len_res"))
                    .zeroWinLen(getCellValueAsLongByHeader(row, "zero_win_len"))
                    .zeroWinLenReq(getCellValueAsLongByHeader(row, "zero_win_len_req"))
                    .zeroWinLenRes(getCellValueAsLongByHeader(row, "zero_win_len_res"))
                    .checksumErrorLen(getCellValueAsLongByHeader(row, "checksum_error_len"))
                    .checksumErrorLenReq(getCellValueAsLongByHeader(row, "checksum_error_len_req"))
                    .checksumErrorLenRes(getCellValueAsLongByHeader(row, "checksum_error_len_res"))

                    // ===== RTT / Count 계열 =====
                    .pageRttConnCntReq(getCellValueAsIntegerByHeader(row, "page_rtt_conn_cnt_req"))
                    .pageRttConnCntRes(getCellValueAsIntegerByHeader(row, "page_rtt_conn_cnt_res"))
                    .pageRttAckCntReq(getCellValueAsIntegerByHeader(row, "page_rtt_ack_cnt_req"))
                    .pageRttAckCntRes(getCellValueAsIntegerByHeader(row, "page_rtt_ack_cnt_res"))
                    .pageReqMakingCnt(getCellValueAsIntegerByHeader(row, "page_req_making_cnt"))
                    .pageHttpCnt(getCellValueAsIntegerByHeader(row, "page_http_cnt"))
                    .pageHttpCntReq(getCellValueAsIntegerByHeader(row, "page_http_cnt_req"))
                    .pageHttpCntRes(getCellValueAsIntegerByHeader(row, "page_http_cnt_res"))
                    .pagePktCnt(getCellValueAsIntegerByHeader(row, "page_pkt_cnt"))
                    .pagePktCntReq(getCellValueAsIntegerByHeader(row, "page_pkt_cnt_req"))
                    .pagePktCntRes(getCellValueAsIntegerByHeader(row, "page_pkt_cnt_res"))
                    .pageSessionCnt(getCellValueAsLongByHeader(row, "page_session_cnt"))
                    .pageTcpConnectCnt(getCellValueAsIntegerByHeader(row, "page_tcp_connect_cnt"))
                    .connErrPktCnt(getCellValueAsIntegerByHeader(row, "conn_err_pkt_cnt"))
                    .connErrSessionCnt(getCellValueAsIntegerByHeader(row, "conn_err_session_cnt"))
                    .retransmissionCnt(getCellValueAsIntegerByHeader(row, "retransmission_cnt"))
                    .retransmissionCntReq(getCellValueAsIntegerByHeader(row, "retransmission_cnt_req"))
                    .retransmissionCntRes(getCellValueAsIntegerByHeader(row, "retransmission_cnt_res"))
                    .outOfOrderCnt(getCellValueAsIntegerByHeader(row, "out_of_order_cnt"))
                    .outOfOrderCntReq(getCellValueAsIntegerByHeader(row, "out_of_order_cnt_req"))
                    .outOfOrderCntRes(getCellValueAsIntegerByHeader(row, "out_of_order_cnt_res"))
                    .lostSegCnt(getCellValueAsIntegerByHeader(row, "lost_seg_cnt"))
                    .lostSegCntReq(getCellValueAsIntegerByHeader(row, "lost_seg_cnt_req"))
                    .lostSegCntRes(getCellValueAsIntegerByHeader(row, "lost_seg_cnt_res"))
                    .ackLostCnt(getCellValueAsIntegerByHeader(row, "ack_lost_cnt"))
                    .ackLostCntReq(getCellValueAsIntegerByHeader(row, "ack_lost_cnt_req"))
                    .ackLostCntRes(getCellValueAsIntegerByHeader(row, "ack_lost_cnt_res"))
                    .winUpdateCnt(getCellValueAsIntegerByHeader(row, "win_update_cnt"))
                    .winUpdateCntReq(getCellValueAsIntegerByHeader(row, "win_update_cnt_req"))
                    .winUpdateCntRes(getCellValueAsIntegerByHeader(row, "win_update_cnt_res"))
                    .dupAckCnt(getCellValueAsIntegerByHeader(row, "dup_ack_cnt"))
                    .dupAckCntReq(getCellValueAsIntegerByHeader(row, "dup_ack_cnt_req"))
                    .dupAckCntRes(getCellValueAsIntegerByHeader(row, "dup_ack_cnt_res"))
                    .zeroWinCnt(getCellValueAsIntegerByHeader(row, "zero_win_cnt"))
                    .zeroWinCntReq(getCellValueAsIntegerByHeader(row, "zero_win_cnt_req"))
                    .zeroWinCntRes(getCellValueAsIntegerByHeader(row, "zero_win_cnt_res"))
                    .windowFullCnt(getCellValueAsIntegerByHeader(row, "window_full_cnt"))
                    .windowFullCntReq(getCellValueAsIntegerByHeader(row, "window_full_cnt_req"))
                    .windowFullCntRes(getCellValueAsIntegerByHeader(row, "window_full_cnt_res"))
                    .pageTcpCnt(getCellValueAsIntegerByHeader(row, "page_tcp_cnt"))
                    .pageTcpCntReq(getCellValueAsIntegerByHeader(row, "page_tcp_cnt_req"))
                    .pageTcpCntRes(getCellValueAsIntegerByHeader(row, "page_tcp_cnt_res"))

                    // ===== HTTP Method Count =====
                    .reqMethodGetCnt(getCellValueAsIntegerByHeader(row, "req_method_get_cnt"))
                    .reqMethodPutCnt(getCellValueAsIntegerByHeader(row, "req_method_put_cnt"))
                    .reqMethodHeadCnt(getCellValueAsIntegerByHeader(row, "req_method_head_cnt"))
                    .reqMethodPostCnt(getCellValueAsIntegerByHeader(row, "req_method_post_cnt"))
                    .reqMethodTraceCnt(getCellValueAsIntegerByHeader(row, "req_method_trace_cnt"))
                    .reqMethodDeleteCnt(getCellValueAsIntegerByHeader(row, "req_method_delete_cnt"))
                    .reqMethodOptionsCnt(getCellValueAsIntegerByHeader(row, "req_method_options_cnt"))
                    .reqMethodPatchCnt(getCellValueAsIntegerByHeader(row, "req_method_patch_cnt"))
                    .reqMethodConnectCnt(getCellValueAsIntegerByHeader(row, "req_method_connect_cnt"))
                    .reqMethodOthCnt(getCellValueAsIntegerByHeader(row, "req_method_oth_cnt"))
                    .reqMethodGetCntError(getCellValueAsIntegerByHeader(row, "req_method_get_cnt_error"))
                    .reqMethodPutCntError(getCellValueAsIntegerByHeader(row, "req_method_put_cnt_error"))
                    .reqMethodHeadCntError(getCellValueAsIntegerByHeader(row, "req_method_head_cnt_error"))
                    .reqMethodPostCntError(getCellValueAsIntegerByHeader(row, "req_method_post_cnt_error"))
                    .reqMethodTraceCntError(getCellValueAsIntegerByHeader(row, "req_method_trace_cnt_error"))
                    .reqMethodDeleteCntError(getCellValueAsIntegerByHeader(row, "req_method_delete_cnt_error"))
                    .reqMethodOptionsCntError(getCellValueAsIntegerByHeader(row, "req_method_options_cnt_error"))
                    .reqMethodPatchCntError(getCellValueAsIntegerByHeader(row, "req_method_patch_cnt_error"))
                    .reqMethodConnectCntError(getCellValueAsIntegerByHeader(row, "req_method_connect_cnt_error"))
                    .reqMethodOthCntError(getCellValueAsIntegerByHeader(row, "req_method_oth_cnt_error"))

                    // ===== Response Code Count =====
                    .resCode1xxCnt(getCellValueAsIntegerByHeader(row, "res_code_1xx_cnt"))
                    .resCode2xxCnt(getCellValueAsIntegerByHeader(row, "res_code_2xx_cnt"))
                    .resCode304Cnt(getCellValueAsIntegerByHeader(row, "res_code_304_cnt"))
                    .resCode3xxCnt(getCellValueAsIntegerByHeader(row, "res_code_3xx_cnt"))
                    .resCode401Cnt(getCellValueAsIntegerByHeader(row, "res_code_401_cnt"))
                    .resCode403Cnt(getCellValueAsIntegerByHeader(row, "res_code_403_cnt"))
                    .resCode404Cnt(getCellValueAsIntegerByHeader(row, "res_code_404_cnt"))
                    .resCode4xxCnt(getCellValueAsIntegerByHeader(row, "res_code_4xx_cnt"))
                    .resCode5xxCnt(getCellValueAsIntegerByHeader(row, "res_code_5xx_cnt"))
                    .resCodeOthCnt(getCellValueAsIntegerByHeader(row, "res_code_oth_cnt"))

                    // ===== Transaction / Timeout etc =====
                    .stoppedTransactionCnt(getCellValueAsIntegerByHeader(row, "stopped_transaction_cnt"))
                    .stoppedTransactionCntReq(getCellValueAsIntegerByHeader(row, "stopped_transaction_cnt_req"))
                    .stoppedTransactionCntRes(getCellValueAsIntegerByHeader(row, "stopped_transaction_cnt_res"))
                    .incompleteCnt(getCellValueAsIntegerByHeader(row, "incomplete_cnt"))
                    .incompleteCntReq(getCellValueAsIntegerByHeader(row, "incomplete_cnt_req"))
                    .incompleteCntRes(getCellValueAsIntegerByHeader(row, "incomplete_cnt_res"))
                    .timeoutCnt(getCellValueAsIntegerByHeader(row, "timeout_cnt"))
                    .timeoutCntReq(getCellValueAsIntegerByHeader(row, "timeout_cnt_req"))
                    .timeoutCntRes(getCellValueAsIntegerByHeader(row, "timeout_cnt_res"))
                    .tsPageRtoCntReq(getCellValueAsIntegerByHeader(row, "ts_page_rto_cnt_req"))
                    .tsPageRtoCntRes(getCellValueAsIntegerByHeader(row, "ts_page_rto_cnt_res"))
                    .tcpErrorCnt(getCellValueAsIntegerByHeader(row, "tcp_error_cnt"))
                    .tcpErrorCntReq(getCellValueAsIntegerByHeader(row, "tcp_error_cnt_req"))
                    .tcpErrorCntRes(getCellValueAsIntegerByHeader(row, "tcp_error_cnt_res"))
                    .tcpErrorLen(getCellValueAsLongByHeader(row, "tcp_error_len"))
                    .tcpErrorLenReq(getCellValueAsLongByHeader(row, "tcp_error_len_req"))
                    .tcpErrorLenRes(getCellValueAsLongByHeader(row, "tcp_error_len_res"))
                    .pageErrorCnt(getCellValueAsIntegerByHeader(row, "page_error_cnt"))
                    .uriCnt(getCellValueAsIntegerByHeader(row, "uri_cnt"))
                    .httpUriCnt(getCellValueAsIntegerByHeader(row, "http_uri_cnt"))
                    .httpsUriCnt(getCellValueAsIntegerByHeader(row, "https_uri_cnt"))

                    // ===== Content-Type Count =====
                    .contentTypeHtmlCntReq(getCellValueAsIntegerByHeader(row, "content_type_html_cnt_req"))
                    .contentTypeHtmlCntRes(getCellValueAsIntegerByHeader(row, "content_type_html_cnt_res"))
                    .contentTypeCssCntReq(getCellValueAsIntegerByHeader(row, "content_type_css_cnt_req"))
                    .contentTypeCssCntRes(getCellValueAsIntegerByHeader(row, "content_type_css_cnt_res"))
                    .contentTypeJsCntReq(getCellValueAsIntegerByHeader(row, "content_type_js_cnt_req"))
                    .contentTypeJsCntRes(getCellValueAsIntegerByHeader(row, "content_type_js_cnt_res"))
                    .contentTypeImgCntReq(getCellValueAsIntegerByHeader(row, "content_type_img_cnt_req"))
                    .contentTypeImgCntRes(getCellValueAsIntegerByHeader(row, "content_type_img_cnt_res"))
                    .contentTypeOthCntReq(getCellValueAsIntegerByHeader(row, "content_type_oth_cnt_req"))
                    .contentTypeOthCntRes(getCellValueAsIntegerByHeader(row, "content_type_oth_cnt_res"))

                    // ===== HTTP/HTTPS / 코드 =====
                    .httpResCode(getCellValueByHeader(row, "http_res_code"))
                    .isHttps(getCellValueAsIntegerByHeader(row, "is_https"))

                    // ===== 세부 타이밍 (ts_*) =====
                    .tsFirst(getCellValueAsDoubleByHeader(row, "ts_first"))
                    .tsPageBegin(getCellValueAsDoubleByHeader(row, "ts_page_begin"))
                    .tsPageEnd(getCellValueAsDoubleByHeader(row, "ts_page_end"))
                    .tsPageReqSyn(getCellValueAsDoubleByHeader(row, "ts_page_req_syn"))
                    .tsPage(getCellValueAsDoubleByHeader(row, "ts_page"))
                    .tsPageGap(getCellValueAsDoubleByHeader(row, "ts_page_gap"))
                    .tsPageResInit(getCellValueAsDoubleByHeader(row, "ts_page_res_init"))
                    .tsPageResInitGap(getCellValueAsDoubleByHeader(row, "ts_page_res_init_gap"))
                    .tsPageResApp(getCellValueAsDoubleByHeader(row, "ts_page_res_app"))
                    .tsPageResAppGap(getCellValueAsDoubleByHeader(row, "ts_page_res_app_gap"))
                    .tsPageRes(getCellValueAsDoubleByHeader(row, "ts_page_res"))
                    .tsPageResGap(getCellValueAsDoubleByHeader(row, "ts_page_res_gap"))
                    .tsPageTransferReq(getCellValueAsDoubleByHeader(row, "ts_page_transfer_req"))
                    .tsPageTransferReqGap(getCellValueAsDoubleByHeader(row, "ts_page_transfer_req_gap"))
                    .tsPageTransferRes(getCellValueAsDoubleByHeader(row, "ts_page_transfer_res"))
                    .tsPageTransferResGap(getCellValueAsDoubleByHeader(row, "ts_page_transfer_res_gap"))
                    .tsPageReqMakingSum(getCellValueAsDoubleByHeader(row, "ts_page_req_making_sum"))
                    .tsPageReqMakingAvg(getCellValueAsDoubleByHeader(row, "ts_page_req_making_avg"))
                    .tsPageTcpConnectSum(getCellValueAsDoubleByHeader(row, "ts_page_tcp_connect_sum"))
                    .tsPageTcpConnectMin(getCellValueAsDoubleByHeader(row, "ts_page_tcp_connect_min"))
                    .tsPageTcpConnectMax(getCellValueAsDoubleByHeader(row, "ts_page_tcp_connect_max"))
                    .tsPageTcpConnectAvg(getCellValueAsDoubleByHeader(row, "ts_page_tcp_connect_avg"))

                    // ===== Mbps / Pps =====
                    .mbps(getCellValueAsDoubleByHeader(row, "mbps"))
                    .mbpsReq(getCellValueAsDoubleByHeader(row, "mbps_req"))
                    .mbpsRes(getCellValueAsDoubleByHeader(row, "mbps_res"))
                    .pps(getCellValueAsDoubleByHeader(row, "pps"))
                    .ppsReq(getCellValueAsDoubleByHeader(row, "pps_req"))
                    .ppsRes(getCellValueAsDoubleByHeader(row, "pps_res"))
                    .mbpsMin(getCellValueAsDoubleByHeader(row, "mbps_min"))
                    .mbpsMinReq(getCellValueAsDoubleByHeader(row, "mbps_min_req"))
                    .mbpsMinRes(getCellValueAsDoubleByHeader(row, "mbps_min_res"))
                    .ppsMin(getCellValueAsDoubleByHeader(row, "pps_min"))
                    .ppsMinReq(getCellValueAsDoubleByHeader(row, "pps_min_req"))
                    .ppsMinRes(getCellValueAsDoubleByHeader(row, "pps_min_res"))
                    .mbpsMax(getCellValueAsDoubleByHeader(row, "mbps_max"))
                    .mbpsMaxReq(getCellValueAsDoubleByHeader(row, "mbps_max_req"))
                    .mbpsMaxRes(getCellValueAsDoubleByHeader(row, "mbps_max_res"))
                    .ppsMax(getCellValueAsDoubleByHeader(row, "pps_max"))
                    .ppsMaxReq(getCellValueAsDoubleByHeader(row, "pps_max_req"))
                    .ppsMaxRes(getCellValueAsDoubleByHeader(row, "pps_max_res"))

                    // ===== Error Percentage =====
                    .tcpErrorPercentage(getCellValueAsDoubleByHeader(row, "tcp_error_percentage"))
                    .tcpErrorPercentageReq(getCellValueAsDoubleByHeader(row, "tcp_error_percentage_req"))
                    .tcpErrorPercentageRes(getCellValueAsDoubleByHeader(row, "tcp_error_percentage_res"))
                    .pageErrorPercentage(getCellValueAsDoubleByHeader(row, "page_error_percentage"))

                    // ===== 위치 정보 =====
                    .countryNameReq(getCellValueByHeader(row, "country_name_req"))
                    .countryNameRes(getCellValueByHeader(row, "country_name_res"))
                    .continentNameReq(getCellValueByHeader(row, "continent_name_req"))
                    .continentNameRes(getCellValueByHeader(row, "continent_name_res"))
                    .domesticPrimaryNameReq(getCellValueByHeader(row, "domestic_primary_name_req"))
                    .domesticPrimaryNameRes(getCellValueByHeader(row, "domestic_primary_name_res"))
                    .domesticSub1NameReq(getCellValueByHeader(row, "domestic_sub1_name_req"))
                    .domesticSub1NameRes(getCellValueByHeader(row, "domestic_sub1_name_res"))
                    .domesticSub2NameReq(getCellValueByHeader(row, "domestic_sub2_name_req"))
                    .domesticSub2NameRes(getCellValueByHeader(row, "domestic_sub2_name_res"))

                    // ===== 프로토콜 / 센서 =====
                    .ndpiProtocolApp(getCellValueByHeader(row, "ndpi_protocol_app"))
                    .ndpiProtocolMaster(getCellValueByHeader(row, "ndpi_protocol_master"))
                    .sensorDeviceName(getCellValueByHeader(row, "sensor_device_name"))

                    // ===== HTTP 세부 필드 =====
                    .httpMethod(getCellValueByHeader(row, "http_method"))
                    .httpVersion(getCellValueByHeader(row, "http_version"))
                    .httpVersionReq(getCellValueByHeader(row, "http_version_req"))
                    .httpVersionRes(getCellValueByHeader(row, "http_version_res"))
                    .httpResPhrase(getCellValueByHeader(row, "http_res_phrase"))
                    .httpContentType(getCellValueByHeader(row, "http_content_type"))
                    .httpUserAgent(getCellValueByHeader(row, "http_user_agent"))
                    .httpCookie(getCellValueByHeader(row, "http_cookie"))
                    .httpLocation(getCellValueByHeader(row, "http_location"))
                    .httpHost(getCellValueByHeader(row, "http_host"))
                    .httpUri(getCellValueByHeader(row, "http_uri"))
                    .httpUriSplit(getCellValueByHeader(row, "http_uri_split"))
                    .httpReferer(getCellValueByHeader(row, "http_referer"))

                    // ===== User Agent 파싱 =====
                    .userAgentSoftwareName(getCellValueByHeader(row, "user_agent_software_name"))
                    .userAgentOperatingSystemName(getCellValueByHeader(row, "user_agent_operating_system_name"))
                    .userAgentOperatingPlatform(getCellValueByHeader(row, "user_agent_operating_platform"))
                    .userAgentSoftwareType(getCellValueByHeader(row, "user_agent_software_type"))
                    .userAgentHardwareType(getCellValueByHeader(row, "user_agent_hardware_type"))
                    .userAgentLayoutEngineName(getCellValueByHeader(row, "user_agent_layout_engine_name"))

                    // ts_server / created_at 은 코드에서 now()로 세팅한다고 치고 여기서는 생략
                    .createdAt(LocalDateTime.now())
                    .build();

        } catch (Exception e) {
            log.warn("행 파싱 실패: {}", e.getMessage());
            return null;
        }
    }

    // 배치로 데이터 읽기
    public List<HttpPageSample> readNextBatch(int size) {
        if (allData.isEmpty()) {
            log.warn("데이터가 없습니다");
            return new ArrayList<>();
        }

        List<HttpPageSample> batch = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            if (currentIndex >= allData.size()) {
                if (loopMode) {
                    currentIndex = 0; // 처음부터 다시
                    log.info("데이터 순환 - 처음부터 다시 시작");
                } else {
                    log.info("모든 데이터 전송 완료");
                    break;
                }
            }

            HttpPageSample sample = allData.get(currentIndex);
            // 타임스탬프를 현재 시간으로 업데이트 (실시간처럼 보이게)
            sample.setTsServer(LocalDateTime.now());
            sample.setCreatedAt(LocalDateTime.now());
            // row_key 새로 생성 (중복 방지)
            sample.setRowKey(UUID.randomUUID().toString());

            batch.add(sample);
            currentIndex++;
        }

        return batch;
    }

    public boolean hasNext() {
        return loopMode || currentIndex < allData.size();
    }

    public int getCurrentIndex() {
        return currentIndex;
    }

    public int getTotalSize() {
        return allData.size();
    }

    // ============== 헤더 이름으로 값 가져오기 ==============

    private String getCellValueByHeader(Row row, String headerName) {
        Integer colIndex = headerIndexMap.get(headerName);
        if (colIndex == null) {
            log.warn("헤더를 찾을 수 없음: {}", headerName);
            return "";
        }
        return getCellValueAsString(row, colIndex);
    }

    private Integer getCellValueAsIntegerByHeader(Row row, String headerName) {
        Integer colIndex = headerIndexMap.get(headerName);
        if (colIndex == null) {
            log.warn("헤더를 찾을 수 없음: {}", headerName);
            return 0;
        }
        return getCellValueAsInteger(row, colIndex);
    }

    private Long getCellValueAsLongByHeader(Row row, String headerName) {
        Integer colIndex = headerIndexMap.get(headerName);
        if (colIndex == null) {
            log.warn("헤더를 찾을 수 없음: {}", headerName);
            return 0L;
        }
        return getCellValueAsLong(row, colIndex);
    }

    private Double getCellValueAsDoubleByHeader(Row row, String headerName) {
        Integer colIndex = headerIndexMap.get(headerName);
        if (colIndex == null) {
            log.warn("헤더를 찾을 수 없음: {}", headerName);
            return 0.0;
        }
        return getCellValueAsDouble(row, colIndex);
    }

    // ============== 실제 셀 값 읽기 헬퍼 메서드 ==============

    private String getCellValueAsString(Row row, int cellIndex) {
        Cell cell = row.getCell(cellIndex);
        if (cell == null) return "";

        DataFormatter formatter = new DataFormatter();

        switch (cell.getCellType()) {
            case STRING:
                return cell.getStringCellValue().trim();
            case NUMERIC:
                return formatter.formatCellValue(cell).trim();
            case BOOLEAN:
                return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                try {
                    return cell.getStringCellValue().trim();
                } catch (IllegalStateException e) {
                    return formatter.formatCellValue(cell).trim();
                }
            default:
                return "";
        }
    }

    private Integer getCellValueAsInteger(Row row, int cellIndex) {
        Cell cell = row.getCell(cellIndex);
        if (cell == null) return 0;

        try {
            if (cell.getCellType() == CellType.NUMERIC) {
                return (int) cell.getNumericCellValue();
            } else if (cell.getCellType() == CellType.STRING) {
                String value = cell.getStringCellValue().trim();
                return value.isEmpty() ? 0 : Integer.parseInt(value);
            }
        } catch (Exception e) {
            log.warn("Integer 변환 실패: {}", e.getMessage());
        }
        return 0;
    }

    private Long getCellValueAsLong(Row row, int cellIndex) {
        Cell cell = row.getCell(cellIndex);
        if (cell == null) return 0L;

        try {
            if (cell.getCellType() == CellType.NUMERIC) {
                return (long) cell.getNumericCellValue();
            } else if (cell.getCellType() == CellType.STRING) {
                String value = cell.getStringCellValue().trim();
                return value.isEmpty() ? 0L : Long.parseLong(value);
            }
        } catch (Exception e) {
            log.warn("Long 변환 실패: {}", e.getMessage());
        }
        return 0L;
    }

    private Double getCellValueAsDouble(Row row, int cellIndex) {
        Cell cell = row.getCell(cellIndex);
        if (cell == null) return 0.0;

        try {
            if (cell.getCellType() == CellType.NUMERIC) {
                return cell.getNumericCellValue();
            } else if (cell.getCellType() == CellType.STRING) {
                String value = cell.getStringCellValue().trim();
                return value.isEmpty() ? 0.0 : Double.parseDouble(value);
            }
        } catch (Exception e) {
            log.warn("Double 변환 실패: {}", e.getMessage());
        }
        return 0.0;
    }
}