package com.moa.moadata.reader;

import com.moa.moadata.model.HttpPageSample;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import jakarta.annotation.PostConstruct;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;

@Slf4j
@Component
public class S3DataReader {

    @Value("${moa.data.s3-bucket}")
    private String bucketName;

    @Value("${moa.data.s3-key}")
    private String s3Key;

    @Value("${moa.data.loop-mode}")
    private boolean loopMode;

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${aws.access-key}")
    private String accessKey;

    @Value("${aws.secret-key}")
    private String secretKey;

    private S3Client s3Client;
    private List<HttpPageSample> allData;
    private int currentIndex = 0;
    private Map<String, Integer> headerIndexMap;

    @PostConstruct
    public void init() {
        // ✅ 환경변수에서 읽은 키로 인증
        this.s3Client = S3Client.builder()
                .region(Region.of(awsRegion))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .build();

        try {
            this.allData = readFromS3();
            log.info("✅ S3 파일 로딩 완료: 총 {}개 데이터 (s3://{}/{})",
                    allData.size(), bucketName, s3Key);
        } catch (Exception e) {
            log.error("❌ S3 파일 로딩 실패", e);
            this.allData = new ArrayList<>();
        }
    }

    private List<HttpPageSample> readFromS3() throws IOException {
        List<HttpPageSample> samples = new ArrayList<>();

        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        try (InputStream inputStream = s3Client.getObject(request);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(inputStream, "EUC-KR"))) {

            String headerLine = reader.readLine();
            if (headerLine == null) {
                log.warn("CSV 파일이 비어있습니다");
                return samples;
            }

            String[] headers = headerLine.split(",", -1);
            this.headerIndexMap = buildHeaderIndexMap(headers);
            log.info("📋 헤더 매핑 완료: {}", headerIndexMap.keySet());

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) continue;

                String[] cols = line.split(",", -1);
                CsvRowAccessor accessor = new CsvRowAccessor(cols, headerIndexMap);
                HttpPageSample sample = parseRow(accessor);
                if (sample != null) {
                    samples.add(sample);
                }
            }
        }

        return samples;
    }

    private Map<String, Integer> buildHeaderIndexMap(String[] headers) {
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            if (headers[i] != null) {
                map.put(headers[i].trim(), i);
            }
        }
        return map;
    }

    private HttpPageSample parseRow(RowAccessor row) {
        try {
            return HttpPageSample.builder()
                    .rowKey(row.getString("row_key"))
                    .srcIp(row.getString("src_ip"))
                    .dstIp(row.getString("dst_ip"))
                    .srcPort(row.getInteger("src_port"))
                    .dstPort(row.getInteger("dst_port"))
                    .tsFrameArrival(row.getDouble("ts_frame_arrival"))
                    .tsFrameLandoff(row.getDouble("ts_frame_landoff"))
                    .pageIdx(row.getLong("page_idx"))
                    .tsServerNsec(row.getDouble("ts_server_nsec"))
                    .srcMac(row.getString("src_mac"))
                    .dstMac(row.getString("dst_mac"))
                    .pageHttpLen(row.getLong("page_http_len"))
                    .pageHttpLenReq(row.getLong("page_http_len_req"))
                    .pageHttpLenRes(row.getLong("page_http_len_res"))
                    .pageHttpHeaderLenReq(row.getLong("page_http_header_len_req"))
                    .pageHttpHeaderLenRes(row.getLong("page_http_header_len_res"))
                    .pageHttpContentLenReq(row.getLong("page_http_content_len_req"))
                    .pageHttpContentLenRes(row.getLong("page_http_content_len_res"))
                    .pagePktLen(row.getLong("page_pkt_len"))
                    .pagePktLenReq(row.getLong("page_pkt_len_req"))
                    .pagePktLenRes(row.getLong("page_pkt_len_res"))
                    .pageTcpLen(row.getLong("page_tcp_len"))
                    .pageTcpLenReq(row.getLong("page_tcp_len_req"))
                    .pageTcpLenRes(row.getLong("page_tcp_len_res"))
                    .httpContentLength(row.getLong("http_content_length"))
                    .httpContentLengthReq(row.getLong("http_content_length_req"))
                    .connErrSessionLen(row.getLong("conn_err_session_len"))
                    .reqConnErrSessionLen(row.getLong("req_conn_err_session_len"))
                    .resConnErrSessionLen(row.getLong("res_conn_err_session_len"))
                    .retransmissionLen(row.getLong("retransmission_len"))
                    .retransmissionLenReq(row.getLong("retransmission_len_req"))
                    .retransmissionLenRes(row.getLong("retransmission_len_res"))
                    .outOfOrderLen(row.getLong("out_of_order_len"))
                    .outOfOrderLenReq(row.getLong("out_of_order_len_req"))
                    .outOfOrderLenRes(row.getLong("out_of_order_len_res"))
                    .lostSegLen(row.getLong("lost_seg_len"))
                    .lostSegLenReq(row.getLong("lost_seg_len_req"))
                    .lostSegLenRes(row.getLong("lost_seg_len_res"))
                    .ackLostLen(row.getLong("ack_lost_len"))
                    .ackLostLenReq(row.getLong("ack_lost_len_req"))
                    .ackLostLenRes(row.getLong("ack_lost_len_res"))
                    .winUpdateLen(row.getLong("win_update_len"))
                    .winUpdateLenReq(row.getLong("win_update_len_req"))
                    .winUpdateLenRes(row.getLong("win_update_len_res"))
                    .dupAckLen(row.getLong("dup_ack_len"))
                    .dupAckLenReq(row.getLong("dup_ack_len_req"))
                    .dupAckLenRes(row.getLong("dup_ack_len_res"))
                    .zeroWinLen(row.getLong("zero_win_len"))
                    .zeroWinLenReq(row.getLong("zero_win_len_req"))
                    .zeroWinLenRes(row.getLong("zero_win_len_res"))
                    .checksumErrorLen(row.getLong("checksum_error_len"))
                    .checksumErrorLenReq(row.getLong("checksum_error_len_req"))
                    .checksumErrorLenRes(row.getLong("checksum_error_len_res"))
                    .pageRttConnCntReq(row.getInteger("page_rtt_conn_cnt_req"))
                    .pageRttConnCntRes(row.getInteger("page_rtt_conn_cnt_res"))
                    .pageRttAckCntReq(row.getInteger("page_rtt_ack_cnt_req"))
                    .pageRttAckCntRes(row.getInteger("page_rtt_ack_cnt_res"))
                    .pageReqMakingCnt(row.getInteger("page_req_making_cnt"))
                    .pageHttpCnt(row.getInteger("page_http_cnt"))
                    .pageHttpCntReq(row.getInteger("page_http_cnt_req"))
                    .pageHttpCntRes(row.getInteger("page_http_cnt_res"))
                    .pagePktCnt(row.getInteger("page_pkt_cnt"))
                    .pagePktCntReq(row.getInteger("page_pkt_cnt_req"))
                    .pagePktCntRes(row.getInteger("page_pkt_cnt_res"))
                    .pageSessionCnt(row.getLong("page_session_cnt"))
                    .pageTcpConnectCnt(row.getInteger("page_tcp_connect_cnt"))
                    .connErrPktCnt(row.getInteger("conn_err_pkt_cnt"))
                    .connErrSessionCnt(row.getInteger("conn_err_session_cnt"))
                    .retransmissionCnt(row.getInteger("retransmission_cnt"))
                    .retransmissionCntReq(row.getInteger("retransmission_cnt_req"))
                    .retransmissionCntRes(row.getInteger("retransmission_cnt_res"))
                    .outOfOrderCnt(row.getInteger("out_of_order_cnt"))
                    .outOfOrderCntReq(row.getInteger("out_of_order_cnt_req"))
                    .outOfOrderCntRes(row.getInteger("out_of_order_cnt_res"))
                    .lostSegCnt(row.getInteger("lost_seg_cnt"))
                    .lostSegCntReq(row.getInteger("lost_seg_cnt_req"))
                    .lostSegCntRes(row.getInteger("lost_seg_cnt_res"))
                    .ackLostCnt(row.getInteger("ack_lost_cnt"))
                    .ackLostCntReq(row.getInteger("ack_lost_cnt_req"))
                    .ackLostCntRes(row.getInteger("ack_lost_cnt_res"))
                    .winUpdateCnt(row.getInteger("win_update_cnt"))
                    .winUpdateCntReq(row.getInteger("win_update_cnt_req"))
                    .winUpdateCntRes(row.getInteger("win_update_cnt_res"))
                    .dupAckCnt(row.getInteger("dup_ack_cnt"))
                    .dupAckCntReq(row.getInteger("dup_ack_cnt_req"))
                    .dupAckCntRes(row.getInteger("dup_ack_cnt_res"))
                    .zeroWinCnt(row.getInteger("zero_win_cnt"))
                    .zeroWinCntReq(row.getInteger("zero_win_cnt_req"))
                    .zeroWinCntRes(row.getInteger("zero_win_cnt_res"))
                    .windowFullCnt(row.getInteger("window_full_cnt"))
                    .windowFullCntReq(row.getInteger("window_full_cnt_req"))
                    .windowFullCntRes(row.getInteger("window_full_cnt_res"))
                    .pageTcpCnt(row.getInteger("page_tcp_cnt"))
                    .pageTcpCntReq(row.getInteger("page_tcp_cnt_req"))
                    .pageTcpCntRes(row.getInteger("page_tcp_cnt_res"))
                    .reqMethodGetCnt(row.getInteger("req_method_get_cnt"))
                    .reqMethodPutCnt(row.getInteger("req_method_put_cnt"))
                    .reqMethodHeadCnt(row.getInteger("req_method_head_cnt"))
                    .reqMethodPostCnt(row.getInteger("req_method_post_cnt"))
                    .reqMethodTraceCnt(row.getInteger("req_method_trace_cnt"))
                    .reqMethodDeleteCnt(row.getInteger("req_method_delete_cnt"))
                    .reqMethodOptionsCnt(row.getInteger("req_method_options_cnt"))
                    .reqMethodPatchCnt(row.getInteger("req_method_patch_cnt"))
                    .reqMethodConnectCnt(row.getInteger("req_method_connect_cnt"))
                    .reqMethodOthCnt(row.getInteger("req_method_oth_cnt"))
                    .reqMethodGetCntError(row.getInteger("req_method_get_cnt_error"))
                    .reqMethodPutCntError(row.getInteger("req_method_put_cnt_error"))
                    .reqMethodHeadCntError(row.getInteger("req_method_head_cnt_error"))
                    .reqMethodPostCntError(row.getInteger("req_method_post_cnt_error"))
                    .reqMethodTraceCntError(row.getInteger("req_method_trace_cnt_error"))
                    .reqMethodDeleteCntError(row.getInteger("req_method_delete_cnt_error"))
                    .reqMethodOptionsCntError(row.getInteger("req_method_options_cnt_error"))
                    .reqMethodPatchCntError(row.getInteger("req_method_patch_cnt_error"))
                    .reqMethodConnectCntError(row.getInteger("req_method_connect_cnt_error"))
                    .reqMethodOthCntError(row.getInteger("req_method_oth_cnt_error"))
                    .resCode1xxCnt(row.getInteger("res_code_1xx_cnt"))
                    .resCode2xxCnt(row.getInteger("res_code_2xx_cnt"))
                    .resCode304Cnt(row.getInteger("res_code_304_cnt"))
                    .resCode3xxCnt(row.getInteger("res_code_3xx_cnt"))
                    .resCode401Cnt(row.getInteger("res_code_401_cnt"))
                    .resCode403Cnt(row.getInteger("res_code_403_cnt"))
                    .resCode404Cnt(row.getInteger("res_code_404_cnt"))
                    .resCode4xxCnt(row.getInteger("res_code_4xx_cnt"))
                    .resCode5xxCnt(row.getInteger("res_code_5xx_cnt"))
                    .resCodeOthCnt(row.getInteger("res_code_oth_cnt"))
                    .stoppedTransactionCnt(row.getInteger("stopped_transaction_cnt"))
                    .stoppedTransactionCntReq(row.getInteger("stopped_transaction_cnt_req"))
                    .stoppedTransactionCntRes(row.getInteger("stopped_transaction_cnt_res"))
                    .incompleteCnt(row.getInteger("incomplete_cnt"))
                    .incompleteCntReq(row.getInteger("incomplete_cnt_req"))
                    .incompleteCntRes(row.getInteger("incomplete_cnt_res"))
                    .timeoutCnt(row.getInteger("timeout_cnt"))
                    .timeoutCntReq(row.getInteger("timeout_cnt_req"))
                    .timeoutCntRes(row.getInteger("timeout_cnt_res"))
                    .tsPageRtoCntReq(row.getInteger("ts_page_rto_cnt_req"))
                    .tsPageRtoCntRes(row.getInteger("ts_page_rto_cnt_res"))
                    .tcpErrorCnt(row.getInteger("tcp_error_cnt"))
                    .tcpErrorCntReq(row.getInteger("tcp_error_cnt_req"))
                    .tcpErrorCntRes(row.getInteger("tcp_error_cnt_res"))
                    .tcpErrorLen(row.getLong("tcp_error_len"))
                    .tcpErrorLenReq(row.getLong("tcp_error_len_req"))
                    .tcpErrorLenRes(row.getLong("tcp_error_len_res"))
                    .pageErrorCnt(row.getInteger("page_error_cnt"))
                    .uriCnt(row.getInteger("uri_cnt"))
                    .httpUriCnt(row.getInteger("http_uri_cnt"))
                    .httpsUriCnt(row.getInteger("https_uri_cnt"))
                    .contentTypeHtmlCntReq(row.getInteger("content_type_html_cnt_req"))
                    .contentTypeHtmlCntRes(row.getInteger("content_type_html_cnt_res"))
                    .contentTypeCssCntReq(row.getInteger("content_type_css_cnt_req"))
                    .contentTypeCssCntRes(row.getInteger("content_type_css_cnt_res"))
                    .contentTypeJsCntReq(row.getInteger("content_type_js_cnt_req"))
                    .contentTypeJsCntRes(row.getInteger("content_type_js_cnt_res"))
                    .contentTypeImgCntReq(row.getInteger("content_type_img_cnt_req"))
                    .contentTypeImgCntRes(row.getInteger("content_type_img_cnt_res"))
                    .contentTypeOthCntReq(row.getInteger("content_type_oth_cnt_req"))
                    .contentTypeOthCntRes(row.getInteger("content_type_oth_cnt_res"))
                    .httpResCode(row.getString("http_res_code"))
                    .isHttps(row.getInteger("is_https"))
                    .tsFirst(row.getDouble("ts_first"))
                    .tsPageBegin(row.getDouble("ts_page_begin"))
                    .tsPageEnd(row.getDouble("ts_page_end"))
                    .tsPageReqSyn(row.getDouble("ts_page_req_syn"))
                    .tsPage(row.getDouble("ts_page"))
                    .tsPageGap(row.getDouble("ts_page_gap"))
                    .tsPageResInit(row.getDouble("ts_page_res_init"))
                    .tsPageResInitGap(row.getDouble("ts_page_res_init_gap"))
                    .tsPageResApp(row.getDouble("ts_page_res_app"))
                    .tsPageResAppGap(row.getDouble("ts_page_res_app_gap"))
                    .tsPageRes(row.getDouble("ts_page_res"))
                    .tsPageResGap(row.getDouble("ts_page_res_gap"))
                    .tsPageTransferReq(row.getDouble("ts_page_transfer_req"))
                    .tsPageTransferReqGap(row.getDouble("ts_page_transfer_req_gap"))
                    .tsPageTransferRes(row.getDouble("ts_page_transfer_res"))
                    .tsPageTransferResGap(row.getDouble("ts_page_transfer_res_gap"))
                    .tsPageReqMakingSum(row.getDouble("ts_page_req_making_sum"))
                    .tsPageReqMakingAvg(row.getDouble("ts_page_req_making_avg"))
                    .tsPageTcpConnectSum(row.getDouble("ts_page_tcp_connect_sum"))
                    .tsPageTcpConnectMin(row.getDouble("ts_page_tcp_connect_min"))
                    .tsPageTcpConnectMax(row.getDouble("ts_page_tcp_connect_max"))
                    .tsPageTcpConnectAvg(row.getDouble("ts_page_tcp_connect_avg"))
                    .mbps(row.getDouble("mbps"))
                    .mbpsReq(row.getDouble("mbps_req"))
                    .mbpsRes(row.getDouble("mbps_res"))
                    .pps(row.getDouble("pps"))
                    .ppsReq(row.getDouble("pps_req"))
                    .ppsRes(row.getDouble("pps_res"))
                    .mbpsMin(row.getDouble("mbps_min"))
                    .mbpsMinReq(row.getDouble("mbps_min_req"))
                    .mbpsMinRes(row.getDouble("mbps_min_res"))
                    .ppsMin(row.getDouble("pps_min"))
                    .ppsMinReq(row.getDouble("pps_min_req"))
                    .ppsMinRes(row.getDouble("pps_min_res"))
                    .mbpsMax(row.getDouble("mbps_max"))
                    .mbpsMaxReq(row.getDouble("mbps_max_req"))
                    .mbpsMaxRes(row.getDouble("mbps_max_res"))
                    .ppsMax(row.getDouble("pps_max"))
                    .ppsMaxReq(row.getDouble("pps_max_req"))
                    .ppsMaxRes(row.getDouble("pps_max_res"))
                    .tcpErrorPercentage(row.getDouble("tcp_error_percentage"))
                    .tcpErrorPercentageReq(row.getDouble("tcp_error_percentage_req"))
                    .tcpErrorPercentageRes(row.getDouble("tcp_error_percentage_res"))
                    .pageErrorPercentage(row.getDouble("page_error_percentage"))
                    .countryNameReq(row.getString("country_name_req"))
                    .countryNameRes(row.getString("country_name_res"))
                    .continentNameReq(row.getString("continent_name_req"))
                    .continentNameRes(row.getString("continent_name_res"))
                    .domesticPrimaryNameReq(row.getString("domestic_primary_name_req"))
                    .domesticPrimaryNameRes(row.getString("domestic_primary_name_res"))
                    .domesticSub1NameReq(row.getString("domestic_sub1_name_req"))
                    .domesticSub1NameRes(row.getString("domestic_sub1_name_res"))
                    .domesticSub2NameReq(row.getString("domestic_sub2_name_req"))
                    .domesticSub2NameRes(row.getString("domestic_sub2_name_res"))
                    .ndpiProtocolApp(row.getString("ndpi_protocol_app"))
                    .ndpiProtocolMaster(row.getString("ndpi_protocol_master"))
                    .sensorDeviceName(row.getString("sensor_device_name"))
                    .httpMethod(row.getString("http_method"))
                    .httpVersion(row.getString("http_version"))
                    .httpVersionReq(row.getString("http_version_req"))
                    .httpVersionRes(row.getString("http_version_res"))
                    .httpResPhrase(row.getString("http_res_phrase"))
                    .httpContentType(row.getString("http_content_type"))
                    .httpUserAgent(row.getString("http_user_agent"))
                    .httpCookie(row.getString("http_cookie"))
                    .httpLocation(row.getString("http_location"))
                    .httpHost(row.getString("http_host"))
                    .httpUri(row.getString("http_uri"))
                    .httpUriSplit(row.getString("http_uri_split"))
                    .httpReferer(row.getString("http_referer"))
                    .userAgentSoftwareName(row.getString("user_agent_software_name"))
                    .userAgentOperatingSystemName(row.getString("user_agent_operating_system_name"))
                    .userAgentOperatingPlatform(row.getString("user_agent_operating_platform"))
                    .userAgentSoftwareType(row.getString("user_agent_software_type"))
                    .userAgentHardwareType(row.getString("user_agent_hardware_type"))
                    .userAgentLayoutEngineName(row.getString("user_agent_layout_engine_name"))
                    .createdAt(LocalDateTime.now())
                    .build();
        } catch (Exception e) {
            log.warn("행 파싱 실패: {}", e.getMessage());
            return null;
        }
    }

    public List<HttpPageSample> readNextBatch(int size) {
        if (allData.isEmpty()) {
            log.warn("데이터가 없습니다");
            return new ArrayList<>();
        }

        List<HttpPageSample> batch = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            if (currentIndex >= allData.size()) {
                if (loopMode) {
                    currentIndex = 0;
                    log.info("데이터 순환 - 처음부터 다시 시작");
                } else {
                    log.info("모든 데이터 전송 완료");
                    break;
                }
            }

            HttpPageSample sample = allData.get(currentIndex);
            sample.setTsServer(LocalDateTime.now());
            sample.setCreatedAt(LocalDateTime.now());
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

    // ============== CSV Row 접근 ==============
    private interface RowAccessor {
        String getString(String headerName);
        Integer getInteger(String headerName);
        Long getLong(String headerName);
        Double getDouble(String headerName);
    }

    private class CsvRowAccessor implements RowAccessor {
        private final String[] cols;
        private final Map<String, Integer> headerMap;

        CsvRowAccessor(String[] cols, Map<String, Integer> headerMap) {
            this.cols = cols;
            this.headerMap = headerMap;
        }

        private String getRaw(String headerName) {
            Integer idx = headerMap.get(headerName);
            if (idx == null || idx < 0 || idx >= cols.length) {
                return "";
            }
            String value = cols[idx];
            return value == null ? "" : value.trim();
        }

        @Override
        public String getString(String headerName) {
            return getRaw(headerName);
        }

        @Override
        public Integer getInteger(String headerName) {
            String v = getRaw(headerName);
            if (v.isEmpty()) return 0;
            try {
                return (int) Double.parseDouble(v);
            } catch (NumberFormatException e) {
                return 0;
            }
        }

        @Override
        public Long getLong(String headerName) {
            String v = getRaw(headerName);
            if (v.isEmpty()) return 0L;
            try {
                return (long) Double.parseDouble(v);
            } catch (NumberFormatException e) {
                return 0L;
            }
        }

        @Override
        public Double getDouble(String headerName) {
            String v = getRaw(headerName);
            if (v.isEmpty()) return 0.0;
            try {
                return Double.parseDouble(v);
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
    }
}