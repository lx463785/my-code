package com.ymhx.dataplatform.kafka.hbase;

import java.math.BigDecimal;
import java.util.Date;

public class AdasInfo {

    /***
     * 车辆ID
     */
    private int vehicleId;

    /**
     * 里程
     */
    private BigDecimal mileage;

    /***
     * 车牌号
     */
    private String numberPlate;

    /***
     * 多媒体ID集合
     */
    private String mulitiMediaIDS;

    /***
     * 经度
     */
    private double latitude;

    /***
     * 纬度
     */
    private double longitude;

    /***
     * 报警时间
     */
    private Date alarmTime;

    /***
     * 报警文件标识
     */
    private String alarmNumber;

    /***
     * 终端ID
     */
    private String terminalId;

    /***
     * 报警类型
     */
    private int alarmType;

    /***
     * 限速速度
     */
    private int speedLimit;

    /***
     * 速度
     */
    private BigDecimal speed;

    /***
     * 限速提示速度
     */
    private BigDecimal tSRSpeedLimit;

    /***
     * 预计碰撞时间
     */
    private BigDecimal warningTime;


    /***
     * 车道偏移类型（1：左偏移，2右偏移）
     */
    private int deviateType;

    /***
     * 道路标识超限报警类型（1：限速，2：限高，3：限重）
     */
    private int roadFlagDistinguishType;

    /***
     * ADAS类型（1：私有，2：苏标。）
     */
    private int kind;

    /***
     * 序列号
     */
    private int serialNumber;

    /***
     * 文件数
     */
    private int fileCount;

    /***
     * 其它
     */
    private int other;


    public int getVehicleId() {
        return vehicleId;
    }

    public void setVehicleId(int vehicleId) {
        this.vehicleId = vehicleId;
    }

    public BigDecimal getMileage() {
        return mileage;
    }

    public void setMileage(BigDecimal mileage) {
        this.mileage = mileage;
    }

    public String getNumberPlate() {
        return numberPlate;
    }

    public void setNumberPlate(String numberPlate) {
        this.numberPlate = numberPlate;
    }

    public String getMulitiMediaIDS() {
        return mulitiMediaIDS;
    }

    public void setMulitiMediaIDS(String mulitiMediaIDS) {
        this.mulitiMediaIDS = mulitiMediaIDS;
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

    public Date getAlarmTime() {
        return alarmTime;
    }

    public void setAlarmTime(Date alarmTime) {
        this.alarmTime = alarmTime;
    }

    public String getAlarmNumber() {
        return alarmNumber;
    }

    public void setAlarmNumber(String alarmNumber) {
        this.alarmNumber = alarmNumber;
    }

    public String getTerminalId() {
        return terminalId;
    }

    public void setTerminalId(String terminalId) {
        this.terminalId = terminalId;
    }

    public int getAlarmType() {
        return alarmType;
    }

    public void setAlarmType(int alarmType) {
        this.alarmType = alarmType;
    }

    public int getSpeedLimit() {
        return speedLimit;
    }

    public void setSpeedLimit(int speedLimit) {
        this.speedLimit = speedLimit;
    }

    public BigDecimal getSpeed() {
        return speed;
    }

    public void setSpeed(BigDecimal speed) {
        this.speed = speed;
    }

    public BigDecimal gettSRSpeedLimit() {
        return tSRSpeedLimit;
    }

    public void settSRSpeedLimit(BigDecimal tSRSpeedLimit) {
        this.tSRSpeedLimit = tSRSpeedLimit;
    }

    public BigDecimal getWarningTime() {
        return warningTime;
    }

    public void setWarningTime(BigDecimal warningTime) {
        this.warningTime = warningTime;
    }

    public int getDeviateType() {
        return deviateType;
    }

    public void setDeviateType(int deviateType) {
        this.deviateType = deviateType;
    }

    public int getRoadFlagDistinguishType() {
        return roadFlagDistinguishType;
    }

    public void setRoadFlagDistinguishType(int roadFlagDistinguishType) {
        this.roadFlagDistinguishType = roadFlagDistinguishType;
    }

    public int getKind() {
        return kind;
    }

    public void setKind(int kind) {
        this.kind = kind;
    }

    public int getSerialNumber() {
        return serialNumber;
    }

    public void setSerialNumber(int serialNumber) {
        this.serialNumber = serialNumber;
    }

    public int getFileCount() {
        return fileCount;
    }

    public void setFileCount(int fileCount) {
        this.fileCount = fileCount;
    }

    public int getOther() {
        return other;
    }

    public void setOther(int other) {
        this.other = other;
    }
}
