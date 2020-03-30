package com.ymhx.dataplatform.kafka.pojo;


import java.io.Serializable;

public class VehicleLocation extends VehicleDynamicInfo implements Serializable {


    /***
     * 车辆定位信息
     */
    private VehicleLocalContent data;


    public VehicleLocalContent getData() {
        return data;
    }

    public void setData(VehicleLocalContent data) {
        this.data = data;
    }

    public static class VehicleLocalContent implements Serializable{
        /***
         * 车辆定位信息
         */
        private VehicleLocationMessage vehicleLocationMessage;

        public VehicleLocationMessage getVehicleLocationMessage() {
            return vehicleLocationMessage;
        }

        public void setVehicleLocationMessage(VehicleLocationMessage vehicleLocationMessage) {
            this.vehicleLocationMessage = vehicleLocationMessage;
        }
    }
}
