package com.ymhx.dataplatform.kafka.pojo;


import java.io.Serializable;

/***
 * 车辆动态信息交换
 */
public class VehicleDynamicInfo implements Serializable {

    public String  vehicleNo;

    public byte    vehicleColor;

    public int     dataType;

    public Long    dataLength;


    public String getVehicleNo() {
        return vehicleNo;
    }

    public void setVehicleNo(String vehicleNo) {
        this.vehicleNo = vehicleNo;
    }

    public byte getVehicleColor() {
        return vehicleColor;
    }

    public void setVehicleColor(byte vehicleColor) {
        this.vehicleColor = vehicleColor;
    }

    public int getDataType() {
        return dataType;
    }

    public void setDataType(int dataType) {
        this.dataType = dataType;
    }

    public Long getDataLength() {
        return dataLength;
    }

    public void setDataLength(Long dataLength) {
        this.dataLength = dataLength;
    }

}
