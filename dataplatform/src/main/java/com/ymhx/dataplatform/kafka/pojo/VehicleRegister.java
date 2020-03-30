package com.ymhx.dataplatform.kafka.pojo;

import java.io.Serializable;

public class VehicleRegister extends VehicleDynamicInfo implements Serializable {


    private VehicleInfo data;

    public VehicleInfo getData() {
        return data;
    }

    public void setData(VehicleInfo data) {
        this.data = data;
    }



    public static class VehicleInfo{

        /***
         * 平台唯一ID
         */
        private String  platformId;

        /***
         * 车载终端厂商唯一编码
         */
        private String  producerId;

        /***
         * 终端型号
         */
        private String  terminalModelType;

        /**
         * 终端ID
         */
        private String  terminalId;

        /***
         * 车载终端电话卡
         */
        private String  terminalSIMCode;


        public String getPlatformId() {
            return platformId;
        }

        public void setPlatformId(String platformId) {
            this.platformId = platformId;
        }

        public String getProducerId() {
            return producerId;
        }

        public void setProducerId(String producerId) {
            this.producerId = producerId;
        }

        public String getTerminalModelType() {
            return terminalModelType;
        }

        public void setTerminalModelType(String terminalModelType) {
            this.terminalModelType = terminalModelType;
        }

        public String getTerminalId() {
            return terminalId;
        }

        public void setTerminalId(String terminalId) {
            this.terminalId = terminalId;
        }

        public String getTerminalSIMCode() {
            return terminalSIMCode;
        }

        public void setTerminalSIMCode(String terminalSIMCode) {
            this.terminalSIMCode = terminalSIMCode;
        }

    }
}

