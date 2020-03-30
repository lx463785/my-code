package com.ymhx.dataplatform.kafka.hbase;

import java.util.Date;

public class DmsInfo {

    /***
     * 车辆ID
     */
    private int vehicleId;

    /***
     * 车牌号
     */
    private String numberPlate;

    /***
     * 多媒体ID集合
     */
    private String mediaId;

    /***
     * 纬度
     */
    private double latitude;

    /***
     * 经度
     */
    private double longitude;

    /***
     * 报警类型
     */
    private int alarmType;

    /***
     * 报警速度
     */
    private double speed;

    /***
     * 疲劳驾驶程度
     */
    private int fatigueDegree;

    /***
     * 报警时间
     */
    private Date alarmTime;

    /***
     * 数据类型（1：平台，2：苏标）
     */
    private int kind;

    /***
     * 报警文件标识
     */
    private String alarmNumber;

    /***
     * 终端ID
     */
    private String terminalId;

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

    public String getNumberPlate() {
        return numberPlate;
    }

    public void setNumberPlate(String numberPlate) {
        this.numberPlate = numberPlate;
    }

    public String getMediaId() {
        return mediaId;
    }

    public void setMediaId(String mediaId) {
        this.mediaId = mediaId;
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

    public int getAlarmType() {
        return alarmType;
    }

    public void setAlarmType(int alarmType) {
        this.alarmType = alarmType;
    }

    public double getSpeed() {
        return speed;
    }

    public void setSpeed(double speed) {
        this.speed = speed;
    }

    public int getFatigueDegree() {
        return fatigueDegree;
    }

    public void setFatigueDegree(int fatigueDegree) {
        this.fatigueDegree = fatigueDegree;
    }

    public Date getAlarmTime() {
        return alarmTime;
    }

    public void setAlarmTime(Date alarmTime) {
        this.alarmTime = alarmTime;
    }

    public int getKind() {
        return kind;
    }

    public void setKind(int kind) {
        this.kind = kind;
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
