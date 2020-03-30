package com.ymhx.dataplatform.kafka.domain;


public class MqConstant {


    /***
     * 下级平台登录请求
     */
    public static final String clientLogin = "client_login";

    /***
     * 平台车辆注册请求
     */
    public static final String vehicleRegister = "vehicle_register";

    /***
     * 车辆实时位置
     */
    public static final String vehicleRealTimeLocation = "vehicle_realTime_location";

    /***
     * 车辆定位信息补发
     */
    public static final String vehicleLocationReissue = "vehicle_location_reissue";

    /***
     * 车辆报警信息
     */
    public static final String vehicleAlarmInformation = "vehicle_alarm_information";

    /***
     * 车辆ADAS报警信息
     */
    public static final String vehicleAdasInfo = "vehicle_adas_info";

    /***
     * 车辆DMS报警信息
     */
    public static final String vehicleDmsInfo = "vehicle_dms_info";

    /***
     * 本服务车辆消费者组
     */
    public static final String meConsumerGroupName = "vehicle_put_mysql";

    /**
     * 统计报警信息表
     *
     */
    public static  final  String countAlarmInformation="count_alarm_info";

}