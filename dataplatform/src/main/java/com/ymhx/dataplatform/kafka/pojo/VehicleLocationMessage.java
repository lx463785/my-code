package com.ymhx.dataplatform.kafka.pojo;



import java.io.Serializable;
import java.util.Date;

public class VehicleLocationMessage implements Serializable {

    /***
     * 加密[1是、0否]
     */
    private boolean encrypt;

    /***
     * 上报时间
     */
    private Date    datetime;

    /***
     * 精度
     */
    private long    lon;

    /***
     * 纬度
     */
    private long    lat;

    /***
     * 卫星定位速度
     */
    private int   vec1;

    /**
     * 行驶记录仪速度
     */
    private int   vec2;

    /**
     * 里程
     */
    private long vec3;

    /**
     * 方向
     */
    private int   direction;

    /***
     * 海拔(m)
     */
    private int   altitude;

    /***
     * 车辆状态
     */
    private long state;

    /***
     * 报警状态
     */
    private long alarm;

    /***
     * acc[0:关、1:开]
     */
    private boolean acc;

    /***
     * 纬[0:北纬、1南纬]
     */
    private boolean latitude;

    /***
     * 经[0:东经、1:西经]
     */
    private boolean longitude;

    /***
     * 运营状态[0:运营、1:停运]
     */
    private boolean operationalStatus;

    /***
     * 车辆油路[0正常:、1;断开]
     */
    private boolean oilRoad;

    /***
     * 车辆电路[0:正常、1:断开]
     */
    private boolean electricCircuit;

    /***
     * 车门锁[0:解锁、1:上锁]
     */
    private boolean doorLock;

    /***
     * 紧急报警[0:否、1是]
     */
    private boolean emergencyReport;

    /***
     * 超速报警[0:否、1是]
     */
    private boolean speedAlarm;

    /***
     * 疲劳驾驶[0:否、1是]
     */
    private boolean fatigueDriving;

    /***
     * 预警[0:否、1是]
     */
    private boolean earlyWarning;

    /***
     * GNSS模块故障[0:否、1是]
     */
    private boolean gnssMmoduleMalfunctioning;

    /**
     * GNSS天线故障[0:否、1是]
     */
    private boolean gnssAntennaMissed;

    /***
     * GNSS天线短路[0:否、1是]
     */
    private boolean gnnsAntennaShort;

    /***
     * 终端主电源欠压[0:否、1是]
     */
    private boolean terminalPowerUndervoltage;

    /***
     * 终端主电源[0:否、1是]
     */
    private boolean terminalMainPower;

    /***
     * LCD显示故障[0:否、1是]
     */
    private boolean lcdDailure;

    /***
     * TTS模块故障[0:否、1是]
     */
    private boolean ttsFailure;

    /***
     * 摄像头故障[0:否、1是]
     */
    private boolean cameraFailure;

    /***
     * 当前累计驾驶超时[0:否、1是]
     */
    private boolean drivingTimeOut;

    /***
     * 超时停车[0:否、1是]
     */
    private boolean timeoutParking;

    /***
     * 进出区域[0:否、1是]
     */
    private boolean outTheArea;

    /***
     * 进出路线[0:否、1是]
     */
    private boolean accessRoutes;

    /***
     * 路段行驶时间不足/过长[0:否、1是]
     */
    private boolean longDrivingTime;

    /***
     * 路线偏离报警[0:否、1是]
     */
    private boolean routeDepartureWarning;

    /***
     * 车辆VSS故障[0:否、1是]
     */
    private boolean vehicleVssFailure;

    /***
     * 车辆油量异常[0:否、1是]
     */
    private boolean abnormalFuelVehicles;

    /***
     * 车辆被盗[0:否、1是]
     */
    private boolean vehicleStolen;

    /***
     * 车辆非法点火[0:否、1是]
     */
    private boolean illegalVehicleIgnition;

    /***
     * 车辆非法位移[0:否、1是]
     */
    private boolean illegalDisplacementVehicle;

    public boolean isEncrypt() {
        return encrypt;
    }

    public void setEncrypt(boolean encrypt) {
        this.encrypt = encrypt;
    }

    public Date getDatetime() {
        return datetime;
    }

    public void setDatetime(Date datetime) {
        this.datetime = datetime;
    }

    public long getLon() {
        return lon;
    }

    public void setLon(long lon) {
        this.lon = lon;
    }

    public long getLat() {
        return lat;
    }

    public void setLat(long lat) {
        this.lat = lat;
    }

    public int getVec1() {
        return vec1;
    }

    public void setVec1(int vec1) {
        this.vec1 = vec1;
    }

    public int getVec2() {
        return vec2;
    }

    public void setVec2(int vec2) {
        this.vec2 = vec2;
    }

    public long getVec3() {
        return vec3;
    }

    public void setVec3(long vec3) {
        this.vec3 = vec3;
    }

    public int getDirection() {
        return direction;
    }

    public void setDirection(int direction) {
        this.direction = direction;
    }

    public int getAltitude() {
        return altitude;
    }

    public void setAltitude(int altitude) {
        this.altitude = altitude;
    }

    public long getState() {
        return state;
    }

    public void setState(long state) {
        this.state = state;
    }

    public long getAlarm() {
        return alarm;
    }

    public void setAlarm(long alarm) {
        this.alarm = alarm;
    }

    public boolean isAcc() {
        return acc;
    }

    public void setAcc(boolean acc) {
        this.acc = acc;
    }

    public boolean isLatitude() {
        return latitude;
    }

    public void setLatitude(boolean latitude) {
        this.latitude = latitude;
    }

    public boolean isLongitude() {
        return longitude;
    }

    public void setLongitude(boolean longitude) {
        this.longitude = longitude;
    }

    public boolean isOperationalStatus() {
        return operationalStatus;
    }

    public void setOperationalStatus(boolean operationalStatus) {
        this.operationalStatus = operationalStatus;
    }

    public boolean isOilRoad() {
        return oilRoad;
    }

    public void setOilRoad(boolean oilRoad) {
        this.oilRoad = oilRoad;
    }

    public boolean isElectricCircuit() {
        return electricCircuit;
    }

    public void setElectricCircuit(boolean electricCircuit) {
        this.electricCircuit = electricCircuit;
    }

    public boolean isDoorLock() {
        return doorLock;
    }

    public void setDoorLock(boolean doorLock) {
        this.doorLock = doorLock;
    }

    public boolean isEmergencyReport() {
        return emergencyReport;
    }

    public void setEmergencyReport(boolean emergencyReport) {
        this.emergencyReport = emergencyReport;
    }

    public boolean isSpeedAlarm() {
        return speedAlarm;
    }

    public void setSpeedAlarm(boolean speedAlarm) {
        this.speedAlarm = speedAlarm;
    }

    public boolean isFatigueDriving() {
        return fatigueDriving;
    }

    public void setFatigueDriving(boolean fatigueDriving) {
        this.fatigueDriving = fatigueDriving;
    }

    public boolean isEarlyWarning() {
        return earlyWarning;
    }

    public void setEarlyWarning(boolean earlyWarning) {
        this.earlyWarning = earlyWarning;
    }

    public boolean isGnssMmoduleMalfunctioning() {
        return gnssMmoduleMalfunctioning;
    }

    public void setGnssMmoduleMalfunctioning(boolean gnssMmoduleMalfunctioning) {
        this.gnssMmoduleMalfunctioning = gnssMmoduleMalfunctioning;
    }

    public boolean isGnssAntennaMissed() {
        return gnssAntennaMissed;
    }

    public void setGnssAntennaMissed(boolean gnssAntennaMissed) {
        this.gnssAntennaMissed = gnssAntennaMissed;
    }

    public boolean isGnnsAntennaShort() {
        return gnnsAntennaShort;
    }

    public void setGnnsAntennaShort(boolean gnnsAntennaShort) {
        this.gnnsAntennaShort = gnnsAntennaShort;
    }

    public boolean isTerminalPowerUndervoltage() {
        return terminalPowerUndervoltage;
    }

    public void setTerminalPowerUndervoltage(boolean terminalPowerUndervoltage) {
        this.terminalPowerUndervoltage = terminalPowerUndervoltage;
    }

    public boolean isTerminalMainPower() {
        return terminalMainPower;
    }

    public void setTerminalMainPower(boolean terminalMainPower) {
        this.terminalMainPower = terminalMainPower;
    }

    public boolean isLcdDailure() {
        return lcdDailure;
    }

    public void setLcdDailure(boolean lcdDailure) {
        this.lcdDailure = lcdDailure;
    }

    public boolean isTtsFailure() {
        return ttsFailure;
    }

    public void setTtsFailure(boolean ttsFailure) {
        this.ttsFailure = ttsFailure;
    }

    public boolean isCameraFailure() {
        return cameraFailure;
    }

    public void setCameraFailure(boolean cameraFailure) {
        this.cameraFailure = cameraFailure;
    }

    public boolean isDrivingTimeOut() {
        return drivingTimeOut;
    }

    public void setDrivingTimeOut(boolean drivingTimeOut) {
        this.drivingTimeOut = drivingTimeOut;
    }

    public boolean isTimeoutParking() {
        return timeoutParking;
    }

    public void setTimeoutParking(boolean timeoutParking) {
        this.timeoutParking = timeoutParking;
    }

    public boolean isOutTheArea() {
        return outTheArea;
    }

    public void setOutTheArea(boolean outTheArea) {
        this.outTheArea = outTheArea;
    }

    public boolean isAccessRoutes() {
        return accessRoutes;
    }

    public void setAccessRoutes(boolean accessRoutes) {
        this.accessRoutes = accessRoutes;
    }

    public boolean isLongDrivingTime() {
        return longDrivingTime;
    }

    public void setLongDrivingTime(boolean longDrivingTime) {
        this.longDrivingTime = longDrivingTime;
    }

    public boolean isRouteDepartureWarning() {
        return routeDepartureWarning;
    }

    public void setRouteDepartureWarning(boolean routeDepartureWarning) {
        this.routeDepartureWarning = routeDepartureWarning;
    }

    public boolean isVehicleVssFailure() {
        return vehicleVssFailure;
    }

    public void setVehicleVssFailure(boolean vehicleVssFailure) {
        this.vehicleVssFailure = vehicleVssFailure;
    }

    public boolean isAbnormalFuelVehicles() {
        return abnormalFuelVehicles;
    }

    public void setAbnormalFuelVehicles(boolean abnormalFuelVehicles) {
        this.abnormalFuelVehicles = abnormalFuelVehicles;
    }

    public boolean isVehicleStolen() {
        return vehicleStolen;
    }

    public void setVehicleStolen(boolean vehicleStolen) {
        this.vehicleStolen = vehicleStolen;
    }

    public boolean isIllegalVehicleIgnition() {
        return illegalVehicleIgnition;
    }

    public void setIllegalVehicleIgnition(boolean illegalVehicleIgnition) {
        this.illegalVehicleIgnition = illegalVehicleIgnition;
    }

    public boolean isIllegalDisplacementVehicle() {
        return illegalDisplacementVehicle;
    }

    public void setIllegalDisplacementVehicle(boolean illegalDisplacementVehicle) {
        this.illegalDisplacementVehicle = illegalDisplacementVehicle;
    }
}
