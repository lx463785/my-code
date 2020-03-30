package com.ymhx.dataplatform.kafka.pojo;



import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.Date;

/***
 * 车辆异常报警
 */
@Component
public class VehicleReportAlarm extends VehicleDynamicInfo implements Serializable {



    private VehicleReportContent data;

    public VehicleReportContent getData() {
        return data;
    }

    public void setData(VehicleReportContent data) {
        this.data = data;
    }

    public static class VehicleReportContent{

        /***
         * 报警信息来源
         */
        private byte    warnSrc;

        /***
         * 报警类型
         */
        private int     warnType;

        /***
         * 报警时间
         */
        private Date    warnTime;

        /***
         * 信息ID
         */
        private long     infoId;

        /***
         * 消息长度
         */
        private long     infoLength;

        /***
         * 报警内容
         */
        private String  infoContent;

        public byte getWarnSrc() {
            return warnSrc;
        }

        public void setWarnSrc(byte warnSrc) {
            this.warnSrc = warnSrc;
        }

        public int getWarnType() {
            return warnType;
        }

        public void setWarnType(int warnType) {
            this.warnType = warnType;
        }

        public Date getWarnTime() {
            return warnTime;
        }

        public void setWarnTime(Date warnTime) {
            this.warnTime = warnTime;
        }

        public long getInfoId() {
            return infoId;
        }

        public void setInfoId(long infoId) {
            this.infoId = infoId;
        }

        public long getInfoLength() {
            return infoLength;
        }

        public void setInfoLength(long infoLength) {
            this.infoLength = infoLength;
        }

        public String getInfoContent() {
            return infoContent;
        }

        public void setInfoContent(String infoContent) {
            this.infoContent = infoContent;
        }

    }




}
