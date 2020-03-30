package com.ymhx.dataplatform.kafka.pojo;

import java.math.BigDecimal;
import java.util.Date;

public class DmsInfo {


    /***
     * 车辆ID
     */
    private int vehicleID;

    /***
     * 车牌号
     */
    private String numberPlate;

    /***
     * 多媒体ID集合
     */
    private String mediaID;

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
    private Date time;

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
    private int terminalID;

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

    /***
     * 定位经度
     */
    private double gpsLatitude;

    /***
     * 定位纬度
     */
    private double gpsLongitude;

    /***
     * 定位里程
     */
    private BigDecimal gpsMileage;

    /***
     * 定位速度
     */
    private BigDecimal gpsSpeed;

    /***
     * 定位限速速度
     */
    private BigDecimal gpsSpeedLimit;

    /***
     * 定位海拔
     */
    private int gpsAltitude;

    /***
     * 定位方向
     */
    private BigDecimal gpsDirection;

    public int getVehicleID() {
        return vehicleID;
    }

    public void setVehicleID(int vehicleID) {
        this.vehicleID = vehicleID;
    }

    public String getMediaID() {
        return mediaID;
    }

    public void setMediaID(String mediaID) {
        this.mediaID = mediaID;
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

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
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

    public int getTerminalID() {
        return terminalID;
    }

    public void setTerminalID(int terminalID) {
        this.terminalID = terminalID;
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

    public String getNumberPlate() {
        return numberPlate;
    }

    public void setNumberPlate(String numberPlate) {
        this.numberPlate = numberPlate;
    }

    public double getGpsLatitude() {
        return gpsLatitude;
    }

    public void setGpsLatitude(double gpsLatitude) {
        this.gpsLatitude = gpsLatitude;
    }

    public double getGpsLongitude() {
        return gpsLongitude;
    }

    public void setGpsLongitude(double gpsLongitude) {
        this.gpsLongitude = gpsLongitude;
    }

    public BigDecimal getGpsMileage() {
        return gpsMileage;
    }

    public void setGpsMileage(BigDecimal gpsMileage) {
        this.gpsMileage = gpsMileage;
    }

    public BigDecimal getGpsSpeed() {
        return gpsSpeed;
    }

    public void setGpsSpeed(BigDecimal gpsSpeed) {
        this.gpsSpeed = gpsSpeed;
    }

    public BigDecimal getGpsSpeedLimit() {
        return gpsSpeedLimit;
    }

    public void setGpsSpeedLimit(BigDecimal gpsSpeedLimit) {
        this.gpsSpeedLimit = gpsSpeedLimit;
    }

    public int getGpsAltitude() {
        return gpsAltitude;
    }

    public void setGpsAltitude(int gpsAltitude) {
        this.gpsAltitude = gpsAltitude;
    }

    public BigDecimal getGpsDirection() {
        return gpsDirection;
    }

    public void setGpsDirection(BigDecimal gpsDirection) {
        this.gpsDirection = gpsDirection;
    }
}
