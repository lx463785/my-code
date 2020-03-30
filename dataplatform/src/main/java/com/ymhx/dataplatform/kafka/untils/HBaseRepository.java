package com.ymhx.dataplatform.kafka.untils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ymhx.dataplatform.kafka.config.BigDataConfig;
import com.ymhx.dataplatform.kafka.domain.MqConstant;
import com.ymhx.dataplatform.kafka.pojo.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.beans.Transient;
import java.io.IOException;
import java.io.Serializable;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class HBaseRepository implements  Serializable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Autowired
    private BigDataConfig bigDataConfig;

    private static Configuration configuration = HBaseConfiguration.create();
    //设置连接池
    private static ExecutorService executorServicePoolSize = Executors.newScheduledThreadPool(20);
    private ThreadLocal<Connection> connectionThreadLocal = new ThreadLocal<>();
    private ThreadLocal<Admin> adminThreadLocal = new ThreadLocal<>();
    private static Connection connection = null;
    private static Admin admin = null;
    private final byte[] POSTFIX = new byte[] { 0x00 };

    //region 加载HBase配置信息
    /**
     * 加载HBase配置信息
     */
    private void initConfigurationInfo(){
        Map<String, String> hBaseConfigMap = bigDataConfig.gethBaseConfigMap();
        if(hBaseConfigMap.size() == 0){
            logger.debug(MessageFormat.format("HBase配置信息初始化失败：{0}", JSON.toJSONString(hBaseConfigMap)));
        }else{
            for (Map.Entry<String,String> confEntry : hBaseConfigMap.entrySet()) {
                configuration.set(confEntry.getKey(), confEntry.getValue());
            }
        }
    }
    //endregion

    //region 初始化HBase client admin
    /**
     * 初始化HBase client admin
     *
     */
    private  void initHBaseClientAdmin(){
        try{
            admin = adminThreadLocal.get();
            if(admin == null && connection != null){
                admin = connection.getAdmin();
                adminThreadLocal.set(admin);
            }else{
                logger.debug(MessageFormat.format("创建hBase connection连接 失败：{0}",connection));
            }
        }catch (Exception e){
            logger.error(MessageFormat.format("初始化hBase client admin 客户端管理失败：错误信息：{0}",e));
        }
    }
    //endregion

    //region 判断表是否存在
    public boolean isexist(String tableName) throws IOException {
        boolean flag=false;
            if(admin != null){
                flag = admin.tableExists(TableName.valueOf(tableName));
            }
        return flag;
    }
    //endregion

    //region 初始化HBase资源
    /**
     * 初始化HBase资源
     */
    @PostConstruct
    public void initRepository(){
        try{
            initConfigurationInfo();
            connection = connectionThreadLocal.get();
            if(connection == null){
                connection = ConnectionFactory.createConnection(configuration, executorServicePoolSize);
                connectionThreadLocal.set(connection);
            }
            initHBaseClientAdmin();
        }catch (Exception e){
            logger.error(MessageFormat.format("创建hBase connection 连接失败：{0}",e));
            e.printStackTrace();
        }finally {
            close(admin,null,null);
        }

    }
    //endregion

    //region
    /**
     * 获取table
     * @param tableName 表名
     * @return Table
     */
    private Table getTable(String tableName) {
        try {
            return connection.getTable(TableName.valueOf(tableName));
        }catch (Exception e){
            e.printStackTrace();
            return  null;
        }
    }
    //endregion

    //region 同时创建多张数据表
    /**
     * 同时创建多张数据表
     * @param tableMap  数据表 map<表名,列簇集合>
     * @return
     */
    public boolean createManyTable(Map<String, List<String>> tableMap){
        try{
            if(admin != null){
                for (Map.Entry<String,List<String>> confEntry : tableMap.entrySet()) {
                    createTable(confEntry.getKey(), confEntry.getValue());
                }
            }
        }catch (Exception e){
            logger.error(MessageFormat.format("创建多个表出现未知错误：{0}",e.getMessage()));
            e.printStackTrace();
            return false;
        }finally {
            close(admin,null,null);
        }
        return true;
    }
    //endregion

    //region 创建hbase表和列簇
    /**
     * 创建hbase表和列簇
     * @param tableName   表名
     * @param columnFamily  列簇
     * @return  1：创建成功；0:创建出错；2：创建的表存在
     */
    public int createOneTable (String tableName,String... columnFamily){
        try{
            //创建表，先查看表是否存在，然后在删除重新创建
            if(admin != null){
                return createTable(tableName, Arrays.asList(columnFamily));
            }else{
                logger.error("admin变量没有初始化成功");
                return 0;
            }
        }catch (Exception e){
            logger.debug(MessageFormat.format("创建表失败：{0},错误信息是：{1}",tableName,e.getMessage()));
            e.printStackTrace();
            return 0;
        }finally {
            close(admin,null,null);
        }
    }
    //endregion
    public int createOneTable (String tableName,List<String> list){
        try{
            //创建表，先查看表是否存在，然后在删除重新创建
            if(admin != null){
                return createTable(tableName, list);
            }else{
                logger.error("admin变量没有初始化成功");
                return 0;
            }
        }catch (Exception e){
            logger.debug(MessageFormat.format("创建表失败：{0},错误信息是：{1}",tableName,e.getMessage()));
            e.printStackTrace();
            return 0;
        }finally {
            close(admin,null,null);
        }
    }
    //endregion

    //region 私有方法创建表
    /**
     *
     * @param tableName
     * @param columnFamily
     * @return
     * @throws Exception
     */
    private int createTable(String tableName,List<String> columnFamily) throws Exception {
        if(admin.tableExists(TableName.valueOf(tableName))){
            logger.debug(MessageFormat.format("创建HBase表名：{0} 在HBase数据库中已经存在",tableName));
            return 2;
        }else{
            List<ColumnFamilyDescriptor> familyDescriptors =new ArrayList<>(columnFamily.size());
            for(String column : columnFamily){
                familyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(column)).build());
            }
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                    .setColumnFamilies(familyDescriptors).build();
            admin.createTable(tableDescriptor);
            logger.info(MessageFormat.format("创建表成功：表名：{0}，列簇：{1}",tableName,JSON.toJSONString(columnFamily)));
            return 1;
        }
    }
    //endregion

    //region 插入or 更新记录（单行单列族-多列多值)
    /**
     * 插入or 更新记录（单行单列族-多列多值)
     * @param tableName          表名
     * @param row           行号  唯一
     * @param columnFamily  列簇名称
     * @param columns       多个列
     * @param values        对应多个列的值
     */
    public boolean insertManyColumnRecords(String tableName,String row,String columnFamily,List<String> columns,List<String> values){
        try{
            Table table = getTable(tableName);
            Put put = new Put(Bytes.toBytes(row));
            for(int i = 0; i < columns.size(); i++){
                put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columns.get(i)),Bytes.toBytes(values.get(i)));
                table.put(put);
            }
            logger.info(MessageFormat.format("添加单行单列族-多列多值数据成功：表名：{0},列名：{1},列值：{2}",tableName, JSON.toJSONString(columns),JSON.toJSONString(values)));
            return  true;
        }catch (Exception e){
            e.printStackTrace();
            logger.debug(MessageFormat.format("添加单行单列族-多列多值数据失败：表名：{0}；错误信息是：{1}",tableName,e.getMessage()));
            return false;
        }
    }
    //endregion

    /**
     * 插入or 更新记录（单行单列族-多列多值)
     * @param tableName          表名ITE
     * @param values        对应多个列的值
     */
    public <T> boolean insertManyColumnRecords(String tableName, List<T> values){
        try{
            Table table = getTable(tableName);
            if (tableName.equals(MqConstant.vehicleAlarmInformation)){
                for (T value : values) {
                    VehicleReportAlarm vehicleReportAlarm = (VehicleReportAlarm) value;
                    Put put = new Put((String.valueOf(Long.MAX_VALUE-System.currentTimeMillis())+vehicleReportAlarm.getVehicleNo()).getBytes());
                    List<String> commonattribute = BeanUtils.getAttribute(VehicleDynamicInfo.class);
                    List<String> alarmattr = BeanUtils.getAttribute( VehicleReportAlarm.VehicleReportContent.class);
                    List<Put> putList = new ArrayList<>();
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(0)),Bytes.toBytes(vehicleReportAlarm.getVehicleNo())));
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(1)),Bytes.toBytes(vehicleReportAlarm.getVehicleColor())));
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(2)),Bytes.toBytes(vehicleReportAlarm.getDataType())));
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(3)),Bytes.toBytes(vehicleReportAlarm.getDataLength())));
                    VehicleReportAlarm.VehicleReportContent data = vehicleReportAlarm.getData();
                    putList.add(put.addColumn(Bytes.toBytes("alarm"),Bytes.toBytes(alarmattr.get(0)),Bytes.toBytes(data.getWarnSrc())));
                    putList.add(put.addColumn(Bytes.toBytes("alarm"),Bytes.toBytes(alarmattr.get(1)),Bytes.toBytes(data.getWarnType())));
                    putList.add(put.addColumn(Bytes.toBytes("alarm"),Bytes.toBytes(alarmattr.get(2)),Bytes.toBytes(data.getWarnTime().toString())));
                    putList.add(put.addColumn(Bytes.toBytes("alarm"),Bytes.toBytes(alarmattr.get(3)),Bytes.toBytes(data.getInfoId())));
                    putList.add(put.addColumn(Bytes.toBytes("alarm"),Bytes.toBytes(alarmattr.get(4)),Bytes.toBytes(data.getInfoLength())));
                    putList.add(put.addColumn(Bytes.toBytes("alarm"),Bytes.toBytes(alarmattr.get(5)),Bytes.toBytes(data.getInfoContent())));
                    table.put(putList);
                }
            }else  if (tableName.equals(MqConstant.vehicleLocationReissue)){
                for (T value : values) {
                    VehicleLocationReissue vehicleLocationReissue = (VehicleLocationReissue) value;
                    Put put = new Put((String.valueOf(Long.MAX_VALUE-System.currentTimeMillis())+vehicleLocationReissue.getVehicleNo()).getBytes());
                    List<String> commonattribute = BeanUtils.getAttribute(VehicleDynamicInfo.class);
                    List<String> alarmattr = BeanUtils.getAttribute( VehicleLocationMessage.class);
                    List<Put> putList = new ArrayList<>();
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(0)),Bytes.toBytes(vehicleLocationReissue.getVehicleNo())));
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(1)),Bytes.toBytes(vehicleLocationReissue.getVehicleColor())));
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(2)),Bytes.toBytes(vehicleLocationReissue.getDataType())));
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(3)),Bytes.toBytes(vehicleLocationReissue.getDataLength())));
                    VehicleLocationReissue.VehicleLocationReissueContent data = vehicleLocationReissue.getData();
                    List<VehicleLocationMessage> vehicleLocationMessages = data.getVehicleLocationMessages();
                    for (VehicleLocationMessage vehicleLocationMessage : vehicleLocationMessages) {                                putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(2)),Bytes.toBytes(vehicleLocationMessage.getAlarm())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(0)),Bytes.toBytes(vehicleLocationMessage.isEncrypt())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(1)),Bytes.toBytes(vehicleLocationMessage.getDatetime().toString())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(2)),Bytes.toBytes(vehicleLocationMessage.getLon())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(3)),Bytes.toBytes(vehicleLocationMessage.getLat())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(4)),Bytes.toBytes(vehicleLocationMessage.getVec1())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(5)),Bytes.toBytes(vehicleLocationMessage.getVec2())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(6)),Bytes.toBytes(vehicleLocationMessage.getVec3())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(7)),Bytes.toBytes(vehicleLocationMessage.getDirection())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(8)),Bytes.toBytes(vehicleLocationMessage.getAltitude())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(9)),Bytes.toBytes(vehicleLocationMessage.getState())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(10)),Bytes.toBytes(vehicleLocationMessage.getAlarm())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(11)),Bytes.toBytes(vehicleLocationMessage.isAcc())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(12)),Bytes.toBytes(vehicleLocationMessage.isLatitude())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(13)),Bytes.toBytes(vehicleLocationMessage.isLongitude())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(14)),Bytes.toBytes(vehicleLocationMessage.isOperationalStatus())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(15)),Bytes.toBytes(vehicleLocationMessage.isOilRoad())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(16)),Bytes.toBytes(vehicleLocationMessage.isElectricCircuit())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(17)),Bytes.toBytes(vehicleLocationMessage.isDoorLock())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(18)),Bytes.toBytes(vehicleLocationMessage.isEmergencyReport())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(19)),Bytes.toBytes(vehicleLocationMessage.isSpeedAlarm())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(20)),Bytes.toBytes(vehicleLocationMessage.isFatigueDriving())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(21)),Bytes.toBytes(vehicleLocationMessage.isEarlyWarning())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(22)),Bytes.toBytes(vehicleLocationMessage.isGnssMmoduleMalfunctioning())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(23)),Bytes.toBytes(vehicleLocationMessage.isGnssAntennaMissed())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(24)),Bytes.toBytes(vehicleLocationMessage.isGnnsAntennaShort())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(25)),Bytes.toBytes(vehicleLocationMessage.isTerminalPowerUndervoltage())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(26)),Bytes.toBytes(vehicleLocationMessage.isTerminalMainPower())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(27)),Bytes.toBytes(vehicleLocationMessage.isLcdDailure())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(28)),Bytes.toBytes(vehicleLocationMessage.isTtsFailure())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(29)),Bytes.toBytes(vehicleLocationMessage.isCameraFailure())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(30)),Bytes.toBytes(vehicleLocationMessage.isDrivingTimeOut())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(31)),Bytes.toBytes(vehicleLocationMessage.isTimeoutParking())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(32)),Bytes.toBytes(vehicleLocationMessage.isOutTheArea())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(33)),Bytes.toBytes(vehicleLocationMessage.isAccessRoutes())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(34)),Bytes.toBytes(vehicleLocationMessage.isLongDrivingTime())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(35)),Bytes.toBytes(vehicleLocationMessage.isRouteDepartureWarning())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(36)),Bytes.toBytes(vehicleLocationMessage.isVehicleVssFailure())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(37)),Bytes.toBytes(vehicleLocationMessage.isAbnormalFuelVehicles())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(38)),Bytes.toBytes(vehicleLocationMessage.isVehicleStolen())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(39)),Bytes.toBytes(vehicleLocationMessage.isIllegalVehicleIgnition())));
                        putList.add(put.addColumn(Bytes.toBytes("reissue"),Bytes.toBytes(alarmattr.get(40)),Bytes.toBytes(vehicleLocationMessage.isIllegalDisplacementVehicle())));
                        table.put(putList);
                    }

                }
            }else  if (tableName.equals(MqConstant.vehicleRealTimeLocation)){
                for (T value : values) {
                    VehicleLocation vehicleLocation = (VehicleLocation) value;
                    Put put = new Put((String.valueOf(Long.MAX_VALUE-System.currentTimeMillis())+vehicleLocation.getVehicleNo()).getBytes());
                    List<String> commonattribute = BeanUtils.getAttribute(VehicleDynamicInfo.class);
                    List<String> alarmattr = BeanUtils.getAttribute(VehicleLocationMessage.class);
                    List<Put> putList = new ArrayList<>();
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(0)),Bytes.toBytes(vehicleLocation.getVehicleNo())));
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(1)),Bytes.toBytes(vehicleLocation.getVehicleColor())));
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(2)),Bytes.toBytes(vehicleLocation.getDataType())));
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(3)),Bytes.toBytes(vehicleLocation.getDataLength())));
                    VehicleLocation.VehicleLocalContent vehicleLocalContent = vehicleLocation.getData();
                    VehicleLocationMessage vehicleLocationMessage =vehicleLocalContent.getVehicleLocationMessage();
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(0)),Bytes.toBytes(vehicleLocationMessage.isEncrypt())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(1)),Bytes.toBytes(vehicleLocationMessage.getDatetime().toString())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(2)),Bytes.toBytes(vehicleLocationMessage.getLon())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(3)),Bytes.toBytes(vehicleLocationMessage.getLat())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(4)),Bytes.toBytes(vehicleLocationMessage.getVec1())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(5)),Bytes.toBytes(vehicleLocationMessage.getVec2())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(6)),Bytes.toBytes(vehicleLocationMessage.getVec3())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(7)),Bytes.toBytes(vehicleLocationMessage.getDirection())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(8)),Bytes.toBytes(vehicleLocationMessage.getAltitude())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(9)),Bytes.toBytes(vehicleLocationMessage.getState())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(10)),Bytes.toBytes(vehicleLocationMessage.getAlarm())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(11)),Bytes.toBytes(vehicleLocationMessage.isAcc())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(12)),Bytes.toBytes(vehicleLocationMessage.isLatitude())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(13)),Bytes.toBytes(vehicleLocationMessage.isLongitude())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(14)),Bytes.toBytes(vehicleLocationMessage.isOperationalStatus())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(15)),Bytes.toBytes(vehicleLocationMessage.isOilRoad())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(16)),Bytes.toBytes(vehicleLocationMessage.isElectricCircuit())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(17)),Bytes.toBytes(vehicleLocationMessage.isDoorLock())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(18)),Bytes.toBytes(vehicleLocationMessage.isEmergencyReport())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(19)),Bytes.toBytes(vehicleLocationMessage.isSpeedAlarm())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(20)),Bytes.toBytes(vehicleLocationMessage.isFatigueDriving())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(21)),Bytes.toBytes(vehicleLocationMessage.isEarlyWarning())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(22)),Bytes.toBytes(vehicleLocationMessage.isGnssMmoduleMalfunctioning())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(23)),Bytes.toBytes(vehicleLocationMessage.isGnssAntennaMissed())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(24)),Bytes.toBytes(vehicleLocationMessage.isGnnsAntennaShort())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(25)),Bytes.toBytes(vehicleLocationMessage.isTerminalPowerUndervoltage())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(26)),Bytes.toBytes(vehicleLocationMessage.isTerminalMainPower())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(27)),Bytes.toBytes(vehicleLocationMessage.isLcdDailure())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(28)),Bytes.toBytes(vehicleLocationMessage.isTtsFailure())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(29)),Bytes.toBytes(vehicleLocationMessage.isCameraFailure())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(30)),Bytes.toBytes(vehicleLocationMessage.isDrivingTimeOut())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(31)),Bytes.toBytes(vehicleLocationMessage.isTimeoutParking())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(32)),Bytes.toBytes(vehicleLocationMessage.isOutTheArea())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(33)),Bytes.toBytes(vehicleLocationMessage.isAccessRoutes())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(34)),Bytes.toBytes(vehicleLocationMessage.isLongDrivingTime())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(35)),Bytes.toBytes(vehicleLocationMessage.isRouteDepartureWarning())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(36)),Bytes.toBytes(vehicleLocationMessage.isVehicleVssFailure())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(37)),Bytes.toBytes(vehicleLocationMessage.isAbnormalFuelVehicles())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(38)),Bytes.toBytes(vehicleLocationMessage.isVehicleStolen())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(39)),Bytes.toBytes(vehicleLocationMessage.isIllegalVehicleIgnition())));
                    putList.add(put.addColumn(Bytes.toBytes("location"),Bytes.toBytes(alarmattr.get(40)),Bytes.toBytes(vehicleLocationMessage.isIllegalDisplacementVehicle())));
                    table.put(putList);
                }
            }else  if (tableName.equals(MqConstant.vehicleRegister)){
                for (T value : values) {
                    VehicleRegister vehicleRegister = (VehicleRegister) value;
                    Put put = new Put((String.valueOf(Long.MAX_VALUE-System.currentTimeMillis())+vehicleRegister.getVehicleNo()).getBytes());
                    List<String> commonattribute = BeanUtils.getAttribute(VehicleDynamicInfo.class);
                    List<String> alarmattr = BeanUtils.getAttribute( VehicleRegister.VehicleInfo.class);
                    List<Put> putList = new ArrayList<>();
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(0)),Bytes.toBytes(vehicleRegister.getVehicleNo())));
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(1)),Bytes.toBytes(vehicleRegister.getVehicleColor())));
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(2)),Bytes.toBytes(vehicleRegister.getDataType())));
                    putList.add(put.addColumn(Bytes.toBytes("common"),Bytes.toBytes(commonattribute.get(3)),Bytes.toBytes(vehicleRegister.getDataLength())));
                    VehicleRegister.VehicleInfo vehicleInfo = vehicleRegister.getData();
                    putList.add(put.addColumn(Bytes.toBytes("register"),Bytes.toBytes(alarmattr.get(0)),Bytes.toBytes(vehicleInfo.getPlatformId())));
                    putList.add(put.addColumn(Bytes.toBytes("register"),Bytes.toBytes(alarmattr.get(1)),Bytes.toBytes(vehicleInfo.getProducerId().toString())));
                    putList.add(put.addColumn(Bytes.toBytes("register"),Bytes.toBytes(alarmattr.get(2)),Bytes.toBytes(vehicleInfo.getTerminalModelType())));
                    putList.add(put.addColumn(Bytes.toBytes("register"),Bytes.toBytes(alarmattr.get(3)),Bytes.toBytes(vehicleInfo.getTerminalId())));
                    putList.add(put.addColumn(Bytes.toBytes("register"),Bytes.toBytes(alarmattr.get(4)),Bytes.toBytes(vehicleInfo.getTerminalSIMCode())));
                    table.put(putList);
                }
            }else  if (tableName.equals(MqConstant.vehicleAdasInfo)){
                for (T value : values) {
                    Adasinfo adasinfo = (Adasinfo) value;
                    Put put = new Put((String.valueOf(Long.MAX_VALUE-System.currentTimeMillis())+adasinfo.getVehicleID()).getBytes());
                    List<String> attribute = BeanUtils.getAttribute(Adasinfo.class);

                    List<Put> putList = new ArrayList<>();
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(0)),Bytes.toBytes(adasinfo.getVehicleID())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(1)),Bytes.toBytes(adasinfo.getMileage())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(2)),Bytes.toBytes(adasinfo.getNumberPlate())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(3)),Bytes.toBytes(adasinfo.getMulitiMediaIDS())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(4)),Bytes.toBytes(adasinfo.getLatitude())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(5)),Bytes.toBytes(adasinfo.getLongitude())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(6)),Bytes.toBytes(adasinfo.getGpsTime().toString())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(7)),Bytes.toBytes(adasinfo.getAlarmNumber())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(8)),Bytes.toBytes(adasinfo.getTerminalID())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(9)),Bytes.toBytes(adasinfo.getAlarmType())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(10)),Bytes.toBytes(adasinfo.getSpeedLimit())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(11)),Bytes.toBytes(adasinfo.getSpeed())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(12)),Bytes.toBytes(adasinfo.gettSRSpeedLimit())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(13)),Bytes.toBytes(adasinfo.getWarningTime())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(14)),Bytes.toBytes(adasinfo.getDeviateType())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(15)),Bytes.toBytes(adasinfo.getRoadFlagDistinguishType())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(16)),Bytes.toBytes(adasinfo.getKind())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(17)),Bytes.toBytes(adasinfo.getSerialNumber())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(18)),Bytes.toBytes(adasinfo.getFileCount())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(19)),Bytes.toBytes(adasinfo.getOther())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(20)),Bytes.toBytes(adasinfo.getGpsLatitude())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(21)),Bytes.toBytes(adasinfo.getGpsLongitude())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(22)),Bytes.toBytes(adasinfo.getGpsMileage())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(23)),Bytes.toBytes(adasinfo.getGpsSpeed())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(24)),Bytes.toBytes(adasinfo.getGpsSpeedLimit())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(25)),Bytes.toBytes(adasinfo.getGpsAltitude())));
                    putList.add(put.addColumn(Bytes.toBytes("adas"),Bytes.toBytes(attribute.get(26)),Bytes.toBytes(adasinfo.getGpsDirection())));
                    table.put(putList);
                }
            }else  if (tableName.equals(MqConstant.vehicleDmsInfo)){
                for (T value : values) {
                    DmsInfo dmsInfo = (DmsInfo) value;
                    Put put = new Put((String.valueOf(Long.MAX_VALUE-System.currentTimeMillis())+dmsInfo.getVehicleID()).getBytes());
                    List<String> attribute = BeanUtils.getAttribute(DmsInfo.class);

                    List<Put> putList = new ArrayList<>();
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(0)),Bytes.toBytes(dmsInfo.getVehicleID())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(2)),Bytes.toBytes(dmsInfo.getNumberPlate())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(1)),Bytes.toBytes(dmsInfo.getMediaID())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(4)),Bytes.toBytes(dmsInfo.getLatitude())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(5)),Bytes.toBytes(dmsInfo.getLongitude())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(3)),Bytes.toBytes(dmsInfo.getAlarmType())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(6)),Bytes.toBytes(dmsInfo.getSpeed())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(7)),Bytes.toBytes(dmsInfo.getFatigueDegree())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(8)),Bytes.toBytes(dmsInfo.getTime()==null?"":dmsInfo.getTime().toString())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(9)),Bytes.toBytes(dmsInfo.getKind())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(10)),Bytes.toBytes(dmsInfo.getAlarmNumber())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(11)),Bytes.toBytes(dmsInfo.getTerminalID())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(12)),Bytes.toBytes(dmsInfo.getSerialNumber())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(13)),Bytes.toBytes(dmsInfo.getFileCount())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(14)),Bytes.toBytes(dmsInfo.getOther())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(20)),Bytes.toBytes(dmsInfo.getGpsLatitude())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(21)),Bytes.toBytes(dmsInfo.getGpsLongitude())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(22)),Bytes.toBytes(dmsInfo.getGpsMileage())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(23)),Bytes.toBytes(dmsInfo.getGpsSpeed())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(24)),Bytes.toBytes(dmsInfo.getGpsSpeedLimit())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(25)),Bytes.toBytes(dmsInfo.getGpsAltitude())));
                    putList.add(put.addColumn(Bytes.toBytes("dms"),Bytes.toBytes(attribute.get(26)),Bytes.toBytes(dmsInfo.getGpsDirection())));

                    table.put(putList);
                }
            }
            return  true;
        }catch (Exception e){
            e.printStackTrace();
            logger.debug(MessageFormat.format("添加单行单列族-多列多值数据失败：表名：{0}；错误信息是：{1}",tableName,e.getMessage()));
            return false;
        }
    }
    //endregion

    //region 插入or更新记录（单行单列族-单列单值)
    /**
     * 插入or更新记录（单行单列族-单列单值)
     * @param tableName          表名
     * @param row           行号  唯一
     * @param columnFamily  列簇名称
     * @param column       列名
     * @param value       列的值
     */
    public boolean insertOneColumnRecords(String tableName,String row,String columnFamily,String column,String value){
        try{
            Table table = getTable(tableName);
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
            table.put(put);
            logger.info(MessageFormat.format("添加单行单列族-单列单值数据成功：表名：{0}，列名：{1}，列值：{2}",tableName,column,value));
            return true;
        }catch (Exception e){
            logger.debug(MessageFormat.format("添加单行单列族-单列单值数据失败：表名：{0}，错误信息是：{1}",tableName,e.getMessage()));
            e.printStackTrace();
            return false;
        }
    }
    //endregion

    //region 根据行号删除表中一条记录
    /**
     * 根据行号删除表中一条记录
     * @param tableName      表名
     * @param rowNumber 行号
     */
    public boolean deleteDataByRowNumber(String tableName,String rowNumber){
        try{
            Table table = getTable(tableName);
            Delete delete = new Delete(Bytes.toBytes(rowNumber));
            table.delete(delete);
            logger.info(MessageFormat.format("根据行号删除表中记录成功：表名：{0}，行号：{1}",tableName,rowNumber));
            return true;
        }catch (Exception e){
            e.printStackTrace();
            logger.debug(MessageFormat.format("根据行号删除表中记录失败：表名：{0}，行号：{1}",tableName,rowNumber));
            return false;
        }
    }
    //endregion

    //region 删除列簇下所有数据
    /**
     * 删除列簇下所有数据
     * @param tableName      表名
     * @param columnFamily  列簇
     */
    public boolean deleteDataByColumnFamily(String tableName,String columnFamily){
        try{
            if(!admin.tableExists(TableName.valueOf(tableName))){
                logger.debug(MessageFormat.format("根据行号和列簇名称删除这行列簇相关的数据失败：表名不存在：{0}",tableName));
                return false;
            }
            admin.deleteColumnFamily(TableName.valueOf(tableName),Bytes.toBytes(columnFamily));
            logger.info(MessageFormat.format("删除该表中列簇下所有数据成功：表名：{0},列簇：{1}",tableName,columnFamily));
            return true;
        }catch (Exception e){
            e.printStackTrace();
            logger.debug(MessageFormat.format("删除该表中列簇下所有数据失败：表名：{0},列簇：{1}，错误信息：{2}",tableName,columnFamily,e.getMessage()));
            return false;
        }
    }
    //endregion

    //region 删除指定的列 ->删除最新列,保留旧列。
    /**
     * 删除指定的列 ->删除最新列,保留旧列。
     * 如 相同的rowkey的name列数据 提交两次数据，此方法只会删除最近的数据，保留旧数据
     * @param tableName          表名
     * @param rowNumber     行号
     * @param columnFamily  列簇
     * @param cloumn        列
     */
    public  boolean deleteDataByColumn(String tableName,String rowNumber,String columnFamily,String cloumn){
        try{
            if(!admin.tableExists(TableName.valueOf(tableName))){
                logger.debug(MessageFormat.format("根据行号表名列簇删除指定列 ->删除最新列,保留旧列失败：表名不存在：{0}",tableName));
                return false;
            }
            Table table = getTable(tableName);
            Delete delete = new Delete(rowNumber.getBytes());
            delete.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(cloumn));
            table.delete(delete);
            logger.info(MessageFormat.format("根据行号表名列簇删除指定列 ->删除最新列,保留旧列成功：表名：{0},行号：{1}，列簇：{2}，列：{3}",tableName,rowNumber,columnFamily,cloumn));
            return true;
        }catch (Exception e){
            e.printStackTrace();
            logger.info(MessageFormat.format("根据行号表名列簇删除指定列 ->删除最新列,保留旧列失败：表名：{0},行号：{1}，列簇：{2}，列：{3}，错误信息：{4}",tableName,rowNumber,columnFamily,cloumn,e.getMessage()));
            return false;
        }
    }
    //endregion

    //region 删除指定的列 ->新旧列都会删除
    /**
     * 删除指定的列 ->新旧列都会删除
     * @param tableName          表名
     * @param rowNumber     行号
     * @param columnFamily  列簇
     * @param cloumn        列
     */
    public  boolean deleteDataByAllcolumn(String tableName,String rowNumber,String columnFamily,String cloumn){
        try{
            if(!admin.tableExists(TableName.valueOf(tableName))){
                logger.debug(MessageFormat.format("根据行号表名列簇删除指定列 ->新旧列都会删除失败：表名不存在：{0}",tableName));
                return false;
            }
            Table table = getTable(tableName);

            Delete delete = new Delete(rowNumber.getBytes());
            delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(cloumn));
            table.delete(delete);
            logger.info(MessageFormat.format("根据行号表名列簇删除指定列 ->新旧列都会删除成功：表名：{0},行号：{1}，列簇：{2}，列：{3}",tableName,rowNumber,columnFamily,cloumn));
            return true;
        }catch (Exception e){
            e.printStackTrace();
            logger.error(MessageFormat.format("根据行号表名列簇删除指定列 ->新旧列都会删除失败：表名：{0},行号：{1}，列簇：{2}，列：{3}，错误信息：{4}",tableName,rowNumber,columnFamily,cloumn,e.getMessage()));
            return false;
        }
    }
    //endregion

    //region 删除表
    /**
     * 删除表
     * @param tableName 表名
     */
    public  boolean  deleteTable(String tableName){
        try{
            TableName table = TableName.valueOf(tableName);
            if(admin.tableExists(table)){
                //禁止使用表,然后删除表
                admin.disableTable(table);
                admin.deleteTable(table);
            }
            logger.info(MessageFormat.format("删除表成功:{0}",tableName));
            return true;
        }catch (Exception e){
            e.printStackTrace();
            logger.debug(MessageFormat.format("删除表失败：{0}，错误信息是：{1}",tableName,e.getMessage()));
            return false;
        }finally {
            close(admin,null,null);
        }
    }
    //endregion

    //region 查询所有表名
    /**
     * 查询所有表名
     */
    public List<String> getAllTableNames(){
        List<String> resultList = new ArrayList<>();
        try {
            TableName[] tableNames = admin.listTableNames();
            for(TableName tableName : tableNames){
                resultList.add(tableName.getNameAsString());
            }
            logger.info(MessageFormat.format("查询库中所有表的表名成功:{0}",JSON.toJSONString(resultList)));
        }catch (IOException e) {
            logger.error("获取所有表的表名失败",e);
        }finally {
            close(admin,null,null);
        }
        return resultList;
    }
    //endregion

    //region 根据表名和行号查询数据
    /**
     * 根据表名和行号查询数据
     * @param tableName  表名
     * @param rowNumber 行号
     * @return
     */
    public Map<String,Object> selectOneRowDataMap(String tableName,String rowNumber){
        Map<String,Object> resultMap = new HashMap<>();
        Get get = new Get(Bytes.toBytes(rowNumber));
        Table table = getTable(tableName);
        try{
            Result result = table.get(get);
            if(result !=null && !result.isEmpty()){
                for(Cell cell : result.listCells()){
                    resultMap.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
                    );
                }
            }
            logger.info(MessageFormat.format("根据表名和行号查询数据：表名：{0}，行号：{1}，查询结果：{2}",tableName,rowNumber,JSON.toJSONString(resultMap)));
        }catch (Exception e){
            e.printStackTrace();
            logger.debug(MessageFormat.format("根据表名和行号查询数据失败：表名：{0}，行号：{1}，错误信息：{2}",tableName,rowNumber,e.getMessage()));
        }finally {
            close(null,null,table);
        }
        return resultMap;
    }
    //endregion

    //region 根据不同条件查询数据
    /**
     * 根据不同条件查询数据
     * @param tableName      表名
     * @param columnFamily   列簇
     * @param queryParam     过滤列集合   ("topicFileId,6282")=>("列,值")
     * @param regex          分隔字符
     * @param bool           查询方式：and 或 or | true : and ；false：or
     *
     * @return
     */
    public List<Map<String,Object>> selectTableDataByFilter(String tableName,String columnFamily,List<String> queryParam,String regex,boolean bool){
        Scan scan = new Scan();
        Table table = getTable(tableName);
        FilterList filterList = queryFilterData(columnFamily, queryParam, regex, bool);
        scan.setFilter(filterList);
        return queryData(table,scan);
    }
    //endregion

    //region 分页的根据不同条件查询数据
    /**
     * 分页的根据不同条件查询数据
     * @param tableName         表名
     * @param columnFamily      列簇
     * @param queryParam        过滤列集合   ("topicFileId,6282")=>("列,值")
     * @param regex             分隔字符
     * @param bool              查询方式：and 或 or | true : and ；false：or
     * @param pageSize          每页显示的数量
     * @param lastRow           当前页的最后一行
     * @return
     */
    public List<Map<String,Object>> selectTableDataByFilterPage(String tableName,String columnFamily,List<String> queryParam,String regex,boolean bool,int pageSize,String lastRow){
        Scan scan = new Scan();
        Table table = getTable(tableName);
        FilterList filterList = queryFilterData(columnFamily, queryParam, regex, bool);
        FilterList pageFilterList = handlePageFilterData(scan, pageSize, lastRow);
        pageFilterList.addFilter(filterList);
        scan.setFilter(pageFilterList);
        return queryData(table,scan);
    }
    //endregion

    //region
    /**
     * 处理分页数据
     * @param scan          过滤的数据
     * @param pageSize      每页显示的数量
     * @param lastRowKey    当前页的最后一行（rowKey）
     * @return
     */
    private FilterList handlePageFilterData(Scan scan, int pageSize,String lastRowKey){
        Filter pageFilter = new PageFilter(pageSize);
        FilterList pageFilterList = new FilterList();
        pageFilterList.addFilter(pageFilter);
        if(!StringUtils.isEmpty(lastRowKey)){
            byte[] startRow = Bytes.add(Bytes.toBytes(lastRowKey), POSTFIX);
            scan.setStartRow(startRow);
        }
        return pageFilterList;
    }
    /**
     * 处理查询条件
     * @param columnFamily   列簇
     * @param queryParam     过滤列集合   ("topicFileId,6282")=>("列,值")
     * @param regex          分隔字符
     * @param bool           查询方式：and 或 or | true : and ；false：or
     * @return
     */
    private FilterList queryFilterData(String columnFamily,List<String> queryParam,String regex,boolean bool){
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        if(!bool){
            filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        }
        for(String param: queryParam){
            String[] queryArray = param.split(regex);
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(queryArray[0]), CompareOperator.EQUAL,Bytes.toBytes(queryArray[1]));
            singleColumnValueFilter.setFilterIfMissing(true);
            filterList.addFilter(singleColumnValueFilter);
        }
        return filterList;
    }
    //endregion

    //region 查根据不同条件查询数据,并返回想要的单列 =>返回的列必须是过滤中存在
    /**
     *  查根据不同条件查询数据,并返回想要的单列 =>返回的列必须是过滤中存在
     * @param tableName         表名
     * @param columnFamily      列簇
     * @param queryParam        过滤列集合   ("topicFileId,6282")=>("列,值")
     * @param regex             分隔字符
     * @param column            返回的列
     * @param bool              查询方式：and 或 or | true : and ；false：or
     * @return
     */
    public List<Map<String,Object>> selectColumnValueDataByFilter(String tableName,String columnFamily,List<String> queryParam,String regex,String column,boolean bool){
        Scan scan = new Scan();
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        if(!bool){
            filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        }
        Table table = getTable(tableName);
        for(String param: queryParam){
            String[] queryArray = param.split(regex);
            SingleColumnValueExcludeFilter singleColumnValueExcludeFilter = new SingleColumnValueExcludeFilter(Bytes.toBytes(columnFamily), Bytes.toBytes(queryArray[0]), CompareOperator.EQUAL,Bytes.toBytes(queryArray[1]));
            filterList.addFilter(singleColumnValueExcludeFilter);
            scan.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(queryArray[0]));
        }
        scan.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(column));
        scan.setFilter(filterList);
        return queryData(table,scan);

    }
    //endregion

    //region 查询表中所有数据信息
    /**
     * 查询表中所有数据信息
     * @param tableName 表名
     * @return
     */
    public List<Map<String,Object>> selectTableAllDataMap(String tableName){
        Table table = getTable(tableName);
        Scan scan = new Scan();
        return queryData(table,scan);
    }
    //endregion

    //region 分页查询表中所有数据信息
    /**
     * 分页查询表中所有数据信息
     * @param tableName     表名
     * @param pageSize      每页数量
     * @param lastRow       当前页的最后一行
     * @return
     */
    public List<Map<String,Object>> selectTableAllDataMapPage(String tableName,int pageSize,String lastRow){
        Scan scan = new Scan();
        Table table = getTable(tableName);
        FilterList pageFilterList = handlePageFilterData(scan, pageSize, lastRow);
        scan.setFilter(pageFilterList);
        return queryData(table,scan);
    }
    //endregion

    //region 根据表名和列簇查询所有数据
    /**
     * 根据表名和列簇查询所有数据
     * @param tableName     表名
     * @param columnFamily  列簇
     * @return
     */
    public List<Map<String,Object>> selectTableAllDataMap(String tableName,String columnFamily){
        Table table = getTable(tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(columnFamily));
        return queryData(table,scan);
    }
    //endregion

    //region 根据相同行号和表名及不同的列簇查询数据
    /**
     * 根据相同行号和表名及不同的列簇查询数据
     * @param tableName     表名
     * @param rowNumber     行号
     * @param columnFamily  列簇
     * @return
     */
    public Map<String,Object> selectTableByRowNumberAndColumnFamily(String tableName,String rowNumber,String columnFamily){
        ResultScanner resultScanner = null;
        Map<String,Object> resultMap = new HashMap<>();
        Table table = getTable(tableName);
        try {
            Get get = new Get(Bytes.toBytes(rowNumber));
            get.addFamily(Bytes.toBytes(columnFamily));
            Result result = table.get(get);
            for(Cell cell :result.listCells()){
                resultMap.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
                );
            }
            logger.info(MessageFormat.format("根据行号和列簇查询表中数据信息：表名：{0}，行号：{1}，列簇：{2},查询结果：{3}",tableName,rowNumber,columnFamily,JSON.toJSONString(resultMap)));
        } catch (IOException e) {
            e.printStackTrace();
            logger.info(MessageFormat.format("根据行号和列簇查询表中数据信息：表名：{0}，行号：{1}，列簇：{2},错误信息：{3}",tableName,rowNumber,columnFamily,e.getMessage()));
        }finally {
            close(null,resultScanner,table);
        }
        return resultMap;
    }
    //endregion


    //region 查询某行中单列数据
    /**
     * 查询某行中单列数据
     * @param tableName       表名
     * @param rowNumber       行号
     * @param columnFamily    列簇
     * @param column          列
     * @return
     */
    public String selectColumnValue(String tableName,String rowNumber,String columnFamily,String column){
        Table table = getTable(tableName);
        try {
            Get get = new Get(Bytes.toBytes(rowNumber));
            get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
            Result result = table.get(get);
            logger.info(MessageFormat.format("根据表名、行号、列簇、列查询指定列的值：表名：{0}，行号：{1}，列簇：{2}，列名：{3}，查询结果：{4}",tableName,rowNumber,columnFamily,column,Bytes.toString(result.value())));
            return Bytes.toString(result.value());
        } catch (IOException e) {
            e.printStackTrace();
            logger.info(MessageFormat.format("根据表名、行号、列簇、列查询指定列的值：表名：{0}，行号：{1}，列簇：{2}，列名：{3}，错误信息：{4}",tableName,rowNumber,columnFamily,column,e.getMessage()));
            return "";
        }finally {
            close(null,null,table);
        }
    }
    //endregion

    //region
    private List<Map<String,Object>> queryData(Table table,Scan scan){
        ResultScanner resultScanner =null;
        List<Map<String,Object>> resultList = new ArrayList<>();
        try {
            resultScanner = table.getScanner(scan);
            for(Result result : resultScanner){
                logger.info(MessageFormat.format("查询每条HBase数据的行号：{0}",Bytes.toString(result.getRow())));
                Map<String,Object> resultMap = new HashMap<>();
                resultMap.put("rowKey",Bytes.toString(result.getRow()));
                for(Cell cell :result.listCells()){
                    resultMap.put(Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                            Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength())
                    );
                }
                resultList.add(resultMap);
            }
            logger.info(MessageFormat.format("查询指定表中数据信息：表名：{0}，查询结果：{1}",Bytes.toString(table.getName().getName()),JSON.toJSONString(resultList)));
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug(MessageFormat.format("查询指定表中数据信息：表名：{0}，错误信息：{1}",Bytes.toString(table.getName().getName()),e.getMessage()));
        }finally {
            close(null,resultScanner,table);
        }
        return resultList;
    }
    //endregion

    //region 统计表中数据总数
    /**
     * 统计表中数据总数
     * @param tableName     表名
     * @return
     */
    public int getTableDataCount(String tableName){
        Table table = getTable(tableName);
        Scan scan = new Scan();
        return queryDataCount(table,scan);

    }
    //endregion

    //region 统计表和列簇数据总数
    /**
     * 统计表和列簇数据总数
     * @param tableName       表名
     * @param columnFamily    列簇
     * @return
     */
    public int getTableDataCount(String tableName,String columnFamily){
        Table table = getTable(tableName);
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(columnFamily));
        return queryDataCount(table,scan);
    }
    private int queryDataCount(Table table,Scan scan){
        scan.setFilter(new FirstKeyOnlyFilter());
        ResultScanner resultScanner = null;
        int rowCount = 0;
        try {
            resultScanner = table.getScanner(scan);
            for(Result result : resultScanner){
                rowCount += result.size();
            }
            logger.info(MessageFormat.format("统计全表数据总数：表名：{0}，查询结果：{1}",Bytes.toString(table.getName().getName()),rowCount));
            return rowCount;
        } catch (Exception e) {
            e.printStackTrace();
            logger.debug(MessageFormat.format("查询指定表中数据信息：表名：{0}，错误信息：{1}",Bytes.toString(table.getName().getName()),e.getMessage()));
            return rowCount;
        }finally {
            close(null,resultScanner,table);
        }
    }
    //endregion

    //region 关闭流
    /**
     * 关闭流
     */
    private void close(Admin admin, ResultScanner rs, Table table){
        if(admin != null){
            try {
                admin.close();
            } catch (IOException e) {
                logger.error("关闭Admin失败",e);
            }
        }
        if(rs != null){
            rs.close();
        }
        if(table != null){
            try {
                table.close();
            } catch (IOException e) {
                logger.error("关闭Table失败",e);
            }
        }
    }
    //endregion
}


