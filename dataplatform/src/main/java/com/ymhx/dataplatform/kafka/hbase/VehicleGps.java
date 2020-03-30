package com.ymhx.dataplatform.kafka.hbase;

import java.math.BigDecimal;
import java.util.Date;

/***
 * 车辆定位信息
 */
public class VehicleGps {

    public long         id;
    public int          vehicleId;
    public int          alarmFlags;
    public int          terminalAlarmOtherFlags;
    public int          platformAlarmFlags;
    public int          alarmStageType;
    public int          status;
    public double       latitude;
    public double       longitude;
    public int          altitude;
    public float        gpsSpeed;
    public float        dtcoSpeed;
    public short        direction;
    public Date         gpsTime;
    public double       mileage;
    public double       oilMass;
    public double       currentOilMass;
    public double       currentOilMassPercent;
    public byte         oilStatus;
    public byte         oilType;
    public int          inNetStatusTypeId;
    public int          terminalVideoAlarmFlags;
    public int          abnormalDrivingFlags;
    public int          fatigueDrivingDegree;
    public int          overSpeedMaxSpeed;
    public int          overSpeedDuration;
    public int          videoAlarmUploadCanMake;
    public int          videoAlarmChannelNo;
    public BigDecimal   speedLimit;
    public int          cargoState;
    public int          speedLimitRegionId;
    public int          airConditionerStatus;
    public int          notRunningTime;
    public int          signalFlags;
    public int          reeferTemperature;
    public int          reserved8;
    public int          reserved9;
    public int          reserved10;
    public Date         addTime;
    public byte[]       additionalInfo;
    public String       terminalId;

    public DmsInfo DMS;
    public AdasInfo ADAS;


    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public int getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(int vehicleId) {
        this.vehicleId = vehicleId;
    }

    public int getAlarmFlags() {
        return alarmFlags;
    }

    public void setAlarmFlags(int alarmFlags) {
        this.alarmFlags = alarmFlags;
    }

    public int getTerminalAlarmOtherFlags() {
        return terminalAlarmOtherFlags;
    }

    public void setTerminalAlarmOtherFlags(int terminalAlarmOtherFlags) {
        this.terminalAlarmOtherFlags = terminalAlarmOtherFlags;
    }

    public int getPlatformAlarmFlags() {
        return platformAlarmFlags;
    }

    public void setPlatformAlarmFlags(int platformAlarmFlags) {
        this.platformAlarmFlags = platformAlarmFlags;
    }

    public int getAlarmStageType() {
        return alarmStageType;
    }

    public void setAlarmStageType(int alarmStageType) {
        this.alarmStageType = alarmStageType;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public int getAltitude() {
        return altitude;
    }

    public void setAltitude(int altitude) {
        this.altitude = altitude;
    }

    public float getGpsSpeed() {
        return gpsSpeed;
    }

    public void setGpsSpeed(float gpsSpeed) {
        this.gpsSpeed = gpsSpeed;
    }

    public float getDtcoSpeed() {
        return dtcoSpeed;
    }

    public void setDtcoSpeed(float dtcoSpeed) {
        this.dtcoSpeed = dtcoSpeed;
    }

    public short getDirection() {
        return direction;
    }

    public void setDirection(short direction) {
        this.direction = direction;
    }

    public Date getGpsTime() {
        return gpsTime;
    }

    public void setGpsTime(Date gpsTime) {
        this.gpsTime = gpsTime;
    }

    public double getMileage() {
        return mileage;
    }

    public void setMileage(double mileage) {
        this.mileage = mileage;
    }

    public double getOilMass() {
        return oilMass;
    }

    public void setOilMass(double oilMass) {
        this.oilMass = oilMass;
    }

    public double getCurrentOilMass() {
        return currentOilMass;
    }

    public void setCurrentOilMass(double currentOilMass) {
        this.currentOilMass = currentOilMass;
    }

    public double getCurrentOilMassPercent() {
        return currentOilMassPercent;
    }

    public void setCurrentOilMassPercent(double currentOilMassPercent) {
        this.currentOilMassPercent = currentOilMassPercent;
    }

    public byte getOilStatus() {
        return oilStatus;
    }

    public void setOilStatus(byte oilStatus) {
        this.oilStatus = oilStatus;
    }

    public byte getOilType() {
        return oilType;
    }

    public void setOilType(byte oilType) {
        this.oilType = oilType;
    }

    public int getInNetStatusTypeId() {
        return inNetStatusTypeId;
    }

    public void setInNetStatusTypeId(int inNetStatusTypeId) {
        this.inNetStatusTypeId = inNetStatusTypeId;
    }

    public int getTerminalVideoAlarmFlags() {
        return terminalVideoAlarmFlags;
    }

    public void setTerminalVideoAlarmFlags(int terminalVideoAlarmFlags) {
        this.terminalVideoAlarmFlags = terminalVideoAlarmFlags;
    }

    public int getAbnormalDrivingFlags() {
        return abnormalDrivingFlags;
    }

    public void setAbnormalDrivingFlags(int abnormalDrivingFlags) {
        this.abnormalDrivingFlags = abnormalDrivingFlags;
    }

    public int getFatigueDrivingDegree() {
        return fatigueDrivingDegree;
    }

    public void setFatigueDrivingDegree(int fatigueDrivingDegree) {
        this.fatigueDrivingDegree = fatigueDrivingDegree;
    }

    public int getOverSpeedMaxSpeed() {
        return overSpeedMaxSpeed;
    }

    public void setOverSpeedMaxSpeed(int overSpeedMaxSpeed) {
        this.overSpeedMaxSpeed = overSpeedMaxSpeed;
    }

    public int getOverSpeedDuration() {
        return overSpeedDuration;
    }

    public void setOverSpeedDuration(int overSpeedDuration) {
        this.overSpeedDuration = overSpeedDuration;
    }

    public int getVideoAlarmUploadCanMake() {
        return videoAlarmUploadCanMake;
    }

    public void setVideoAlarmUploadCanMake(int videoAlarmUploadCanMake) {
        this.videoAlarmUploadCanMake = videoAlarmUploadCanMake;
    }

    public int getVideoAlarmChannelNo() {
        return videoAlarmChannelNo;
    }

    public void setVideoAlarmChannelNo(int videoAlarmChannelNo) {
        this.videoAlarmChannelNo = videoAlarmChannelNo;
    }

    public BigDecimal getSpeedLimit() {
        return speedLimit;
    }

    public void setSpeedLimit(BigDecimal speedLimit) {
        this.speedLimit = speedLimit;
    }

    public int getCargoState() {
        return cargoState;
    }

    public void setCargoState(int cargoState) {
        this.cargoState = cargoState;
    }

    public int getSpeedLimitRegionId() {
        return speedLimitRegionId;
    }

    public void setSpeedLimitRegionId(int speedLimitRegionId) {
        this.speedLimitRegionId = speedLimitRegionId;
    }

    public int getAirConditionerStatus() {
        return airConditionerStatus;
    }

    public void setAirConditionerStatus(int airConditionerStatus) {
        this.airConditionerStatus = airConditionerStatus;
    }

    public int getNotRunningTime() {
        return notRunningTime;
    }

    public void setNotRunningTime(int notRunningTime) {
        this.notRunningTime = notRunningTime;
    }

    public int getSignalFlags() {
        return signalFlags;
    }

    public void setSignalFlags(int signalFlags) {
        this.signalFlags = signalFlags;
    }

    public int getReeferTemperature() {
        return reeferTemperature;
    }

    public void setReeferTemperature(int reeferTemperature) {
        this.reeferTemperature = reeferTemperature;
    }

    public int getReserved8() {
        return reserved8;
    }

    public void setReserved8(int reserved8) {
        this.reserved8 = reserved8;
    }

    public int getReserved9() {
        return reserved9;
    }

    public void setReserved9(int reserved9) {
        this.reserved9 = reserved9;
    }

    public int getReserved10() {
        return reserved10;
    }

    public void setReserved10(int reserved10) {
        this.reserved10 = reserved10;
    }

    public Date getAddTime() {
        return addTime;
    }

    public void setAddTime(Date addTime) {
        this.addTime = addTime;
    }

    public byte[] getAdditionalInfo() {
        return additionalInfo;
    }

    public void setAdditionalInfo(byte[] additionalInfo) {
        this.additionalInfo = additionalInfo;
    }

    public String getTerminalId() {
        return terminalId;
    }

    public void setTerminalId(String terminalId) {
        this.terminalId = terminalId;
    }

    public DmsInfo getDMS() {
        return DMS;
    }

    public void setDMS(DmsInfo DMS) {
        this.DMS = DMS;
    }

    public AdasInfo getADAS() {
        return ADAS;
    }

    public void setADAS(AdasInfo ADAS) {
        this.ADAS = ADAS;
    }
}
