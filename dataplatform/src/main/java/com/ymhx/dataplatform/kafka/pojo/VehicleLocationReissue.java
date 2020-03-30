package com.ymhx.dataplatform.kafka.pojo;



import java.io.Serializable;
import java.util.List;

/****
 * 车辆定位信息补发
 */
public class VehicleLocationReissue extends VehicleDynamicInfo implements Serializable {



    private VehicleLocationReissueContent data;

    public VehicleLocationReissueContent getData() {
        return data;
    }

    public void setData(VehicleLocationReissueContent data) {
        this.data = data;
    }

    public static class VehicleLocationReissueContent{

        /***
         * 卫星定位数据个数
         */
        private byte    gnssCnt;

        /***
         * 车辆定位信息
         */
        private List<VehicleLocationMessage> vehicleLocationMessages;


        public byte getGnssCnt() {
            return gnssCnt;
        }

        public void setGnssCnt(byte gnssCnt) {
            this.gnssCnt = gnssCnt;
        }

        public List<VehicleLocationMessage> getVehicleLocationMessages() {
            return vehicleLocationMessages;
        }

        public void setVehicleLocationMessages(List<VehicleLocationMessage> vehicleLocationMessages) {
            this.vehicleLocationMessages = vehicleLocationMessages;
        }

    }


}
