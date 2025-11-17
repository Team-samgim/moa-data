package com.moa.moadata.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class HttpPageSample {

    // Primary Key
    private String rowKey;

    // IP & Port
    private String srcIp;
    private String dstIp;
    private Integer srcPort;
    private Integer dstPort;

    // Timestamp
    private Double tsFrameArrival;
    private Double tsFrameLandoff;
    private Long pageIdx;
    private LocalDateTime tsServer;
    private Double tsServerNsec;

    // MAC Address
    private String srcMac;
    private String dstMac;

    // HTTP Length
    private Long pageHttpLen;
    private Long pageHttpLenReq;
    private Long pageHttpLenRes;
    private Long pageHttpHeaderLenReq;
    private Long pageHttpHeaderLenRes;
    private Long pageHttpContentLenReq;
    private Long pageHttpContentLenRes;

    // Packet Length
    private Long pagePktLen;
    private Long pagePktLenReq;
    private Long pagePktLenRes;

    // TCP Length
    private Long pageTcpLen;
    private Long pageTcpLenReq;
    private Long pageTcpLenRes;
    private Long httpContentLength;
    private Long httpContentLengthReq;

    // Connection Error Session Length
    private Long connErrSessionLen;
    private Long reqConnErrSessionLen;
    private Long resConnErrSessionLen;

    // Retransmission Length
    private Long retransmissionLen;
    private Long retransmissionLenReq;
    private Long retransmissionLenRes;

    // Out of Order Length
    private Long outOfOrderLen;
    private Long outOfOrderLenReq;
    private Long outOfOrderLenRes;

    // Lost Segment Length
    private Long lostSegLen;
    private Long lostSegLenReq;
    private Long lostSegLenRes;

    // ACK Lost Length
    private Long ackLostLen;
    private Long ackLostLenReq;
    private Long ackLostLenRes;

    // Window Update Length
    private Long winUpdateLen;
    private Long winUpdateLenReq;
    private Long winUpdateLenRes;

    // Duplicate ACK Length
    private Long dupAckLen;
    private Long dupAckLenReq;
    private Long dupAckLenRes;

    // Zero Window Length
    private Long zeroWinLen;
    private Long zeroWinLenReq;
    private Long zeroWinLenRes;

    // Checksum Error Length
    private Long checksumErrorLen;
    private Long checksumErrorLenReq;
    private Long checksumErrorLenRes;

    // RTT Count
    private Integer pageRttConnCntReq;
    private Integer pageRttConnCntRes;
    private Integer pageRttAckCntReq;
    private Integer pageRttAckCntRes;

    // Request Making Count
    private Integer pageReqMakingCnt;

    // HTTP Count
    private Integer pageHttpCnt;
    private Integer pageHttpCntReq;
    private Integer pageHttpCntRes;

    // Packet Count
    private Integer pagePktCnt;
    private Integer pagePktCntReq;
    private Integer pagePktCntRes;

    // Session & Connection Count
    private Long pageSessionCnt;
    private Integer pageTcpConnectCnt;

    // Connection Error Count
    private Integer connErrPktCnt;
    private Integer connErrSessionCnt;

    // Retransmission Count
    private Integer retransmissionCnt;
    private Integer retransmissionCntReq;
    private Integer retransmissionCntRes;

    // Out of Order Count
    private Integer outOfOrderCnt;
    private Integer outOfOrderCntReq;
    private Integer outOfOrderCntRes;

    // Lost Segment Count
    private Integer lostSegCnt;
    private Integer lostSegCntReq;
    private Integer lostSegCntRes;

    // ACK Lost Count
    private Integer ackLostCnt;
    private Integer ackLostCntReq;
    private Integer ackLostCntRes;

    // Window Update Count
    private Integer winUpdateCnt;
    private Integer winUpdateCntReq;
    private Integer winUpdateCntRes;

    // Duplicate ACK Count
    private Integer dupAckCnt;
    private Integer dupAckCntReq;
    private Integer dupAckCntRes;

    // Zero Window Count
    private Integer zeroWinCnt;
    private Integer zeroWinCntReq;
    private Integer zeroWinCntRes;

    // Window Full Count
    private Integer windowFullCnt;
    private Integer windowFullCntReq;
    private Integer windowFullCntRes;

    // TCP Count
    private Integer pageTcpCnt;
    private Integer pageTcpCntReq;
    private Integer pageTcpCntRes;

    // Request Method Count
    private Integer reqMethodGetCnt;
    private Integer reqMethodPutCnt;
    private Integer reqMethodHeadCnt;
    private Integer reqMethodPostCnt;
    private Integer reqMethodTraceCnt;
    private Integer reqMethodDeleteCnt;
    private Integer reqMethodOptionsCnt;
    private Integer reqMethodPatchCnt;
    private Integer reqMethodConnectCnt;
    private Integer reqMethodOthCnt;

    // Request Method Error Count
    private Integer reqMethodGetCntError;
    private Integer reqMethodPutCntError;
    private Integer reqMethodHeadCntError;
    private Integer reqMethodPostCntError;
    private Integer reqMethodTraceCntError;
    private Integer reqMethodDeleteCntError;
    private Integer reqMethodOptionsCntError;
    private Integer reqMethodPatchCntError;
    private Integer reqMethodConnectCntError;
    private Integer reqMethodOthCntError;

    // Response Code Count
    private Integer resCode1xxCnt;
    private Integer resCode2xxCnt;
    private Integer resCode304Cnt;
    private Integer resCode3xxCnt;
    private Integer resCode401Cnt;
    private Integer resCode403Cnt;
    private Integer resCode404Cnt;
    private Integer resCode4xxCnt;
    private Integer resCode5xxCnt;
    private Integer resCodeOthCnt;

    // Transaction Count
    private Integer stoppedTransactionCnt;
    private Integer stoppedTransactionCntReq;
    private Integer stoppedTransactionCntRes;

    // Incomplete Count
    private Integer incompleteCnt;
    private Integer incompleteCntReq;
    private Integer incompleteCntRes;

    // Timeout Count
    private Integer timeoutCnt;
    private Integer timeoutCntReq;
    private Integer timeoutCntRes;

    // RTO Count
    private Integer tsPageRtoCntReq;
    private Integer tsPageRtoCntRes;

    // TCP Error
    private Integer tcpErrorCnt;
    private Integer tcpErrorCntReq;
    private Integer tcpErrorCntRes;
    private Long tcpErrorLen;
    private Long tcpErrorLenReq;
    private Long tcpErrorLenRes;

    // Page Error
    private Integer pageErrorCnt;

    // URI Count
    private Integer uriCnt;
    private Integer httpUriCnt;
    private Integer httpsUriCnt;

    // Content Type Count
    private Integer contentTypeHtmlCntReq;
    private Integer contentTypeHtmlCntRes;
    private Integer contentTypeCssCntReq;
    private Integer contentTypeCssCntRes;
    private Integer contentTypeJsCntReq;
    private Integer contentTypeJsCntRes;
    private Integer contentTypeImgCntReq;
    private Integer contentTypeImgCntRes;
    private Integer contentTypeOthCntReq;
    private Integer contentTypeOthCntRes;

    // HTTP Response Code
    private String httpResCode;
    private Integer isHttps;

    // Timing Information (Double for milliseconds precision)
    private Double tsFirst;
    private Double tsPageBegin;
    private Double tsPageEnd;
    private Double tsPageReqSyn;
    private Double tsPage;
    private Double tsPageGap;
    private Double tsPageResInit;
    private Double tsPageResInitGap;
    private Double tsPageResApp;
    private Double tsPageResAppGap;
    private Double tsPageRes;
    private Double tsPageResGap;
    private Double tsPageTransferReq;
    private Double tsPageTransferReqGap;
    private Double tsPageTransferRes;
    private Double tsPageTransferResGap;
    private Double tsPageReqMakingSum;
    private Double tsPageReqMakingAvg;
    private Double tsPageTcpConnectSum;
    private Double tsPageTcpConnectMin;
    private Double tsPageTcpConnectMax;
    private Double tsPageTcpConnectAvg;

    // Network Speed (Mbps/pps)
    private Double mbps;
    private Double mbpsReq;
    private Double mbpsRes;
    private Double pps;
    private Double ppsReq;
    private Double ppsRes;
    private Double mbpsMin;
    private Double mbpsMinReq;
    private Double mbpsMinRes;
    private Double ppsMin;
    private Double ppsMinReq;
    private Double ppsMinRes;
    private Double mbpsMax;
    private Double mbpsMaxReq;
    private Double mbpsMaxRes;
    private Double ppsMax;
    private Double ppsMaxReq;
    private Double ppsMaxRes;

    // Error Percentage
    private Double tcpErrorPercentage;
    private Double tcpErrorPercentageReq;
    private Double tcpErrorPercentageRes;
    private Double pageErrorPercentage;

    // Location Information
    private String countryNameReq;
    private String countryNameRes;
    private String continentNameReq;
    private String continentNameRes;
    private String domesticPrimaryNameReq;
    private String domesticPrimaryNameRes;
    private String domesticSub1NameReq;
    private String domesticSub1NameRes;
    private String domesticSub2NameReq;
    private String domesticSub2NameRes;

    // Protocol Information
    private String ndpiProtocolApp;
    private String ndpiProtocolMaster;
    private String sensorDeviceName;

    // HTTP Information
    private String httpMethod;
    private String httpVersion;
    private String httpVersionReq;
    private String httpVersionRes;
    private String httpResPhrase;
    private String httpContentType;
    private String httpUserAgent;
    private String httpCookie;
    private String httpLocation;
    private String httpHost;
    private String httpUri;
    private String httpUriSplit;
    private String httpReferer;

    // User Agent Information
    private String userAgentSoftwareName;
    private String userAgentOperatingSystemName;
    private String userAgentOperatingPlatform;
    private String userAgentSoftwareType;
    private String userAgentHardwareType;
    private String userAgentLayoutEngineName;

    // Metadata
    private LocalDateTime createdAt;
}