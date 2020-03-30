package com.ymhx.dataplatform.kafka;


import com.alibaba.fastjson.JSON;
import com.ymhx.dataplatform.kafka.config.KafkaConfigMessage;
import com.ymhx.dataplatform.kafka.domain.MqConstant;

import com.ymhx.dataplatform.kafka.domain.ResponseMes;
import com.ymhx.dataplatform.kafka.pojo.*;
import com.ymhx.dataplatform.kafka.untils.BeanUtils;
import com.ymhx.dataplatform.kafka.untils.HBaseRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;


@Component
public class KafkaMesConsumer implements Serializable {

    @Autowired
    private KafkaConfigMessage kafkaConfig;
    @Autowired
    private HBaseRepository hBaseRepository;

    private Logger logger = LoggerFactory.getLogger(this.getClass());


    //region  车辆报警信息

    public ResponseMes getVehicleAlarmInformation() {
        // 构建SparkStreaming上下文
        SparkConf conf = new SparkConf().setAppName("alarm")
                .setMaster("local[2]")
                //序列化
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        // 每隔5秒钟，sparkStreaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 构建kafka参数map
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaConfig.getServers());
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", kafkaConfig.getGroupid());
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);


        // 构建topic set
        Collection<String> topics = new HashSet<>();
        topics.add(MqConstant.vehicleAlarmInformation);

        try {
            // 获取kafka的数据
            final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                            jssc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                    );

            //region 创建Hbase的相应表
            boolean isexist = hBaseRepository.isexist(MqConstant.vehicleAlarmInformation);
            if (!isexist) {
                List<String> list = new ArrayList<>();
                list.add("common");
                list.add("alarm");
                int oneTable = hBaseRepository.createOneTable(MqConstant.vehicleAlarmInformation, list);
                if (oneTable != 1 && oneTable != 2) {
                    logger.error(MqConstant.vehicleAlarmInformation + "创建失败");
                    return new ResponseMes(false, "999", MqConstant.vehicleAlarmInformation + "创建失败");
                }
            }
                //endregion

            stream.map(s -> {
                VehicleReportAlarm vehicleReportAlarm = JSON.parseObject(s.value(), VehicleReportAlarm.class);
                System.out.println(vehicleReportAlarm);
                return JSON.parseObject(s.value(), VehicleReportAlarm.class);
            }).foreachRDD(rdd -> {
                List<VehicleReportAlarm> collect = rdd.collect();

                if (!collect.isEmpty()) {
                        boolean flag = hBaseRepository.insertManyColumnRecords(MqConstant.vehicleAlarmInformation,collect );
                        if (!flag)logger.error("保存Hbase数据失败");
                }
            });
            jssc.start();
            jssc.awaitTermination();
            jssc.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseMes(true, null);
    }
    //endregion

    //region  车辆定位信息补发
    public ResponseMes getVehicleLocationReissue() {
        // 构建SparkStreaming上下文
        SparkConf conf = new SparkConf().setAppName("reissue1")
                .setMaster("local[2]")
                //序列化
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        // 每隔5秒钟，sparkStreaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 构建kafka参数map
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaConfig.getServers());
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", kafkaConfig.getGroupid());
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);


        // 构建topic set
        Collection<String> topics = new HashSet<>();
        topics.add(MqConstant.vehicleLocationReissue);

        try {
            // 获取kafka的数据
            final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
            );

            //region 创建Hbase的相应表
            boolean isexist = hBaseRepository.isexist(MqConstant.vehicleLocationReissue);
            if (!isexist) {
                List<String> list = new ArrayList<>();
                list.add("common");
                list.add("reissue");
                int oneTable = hBaseRepository.createOneTable(MqConstant.vehicleLocationReissue, list);
                if (oneTable != 1 && oneTable != 2) {
                    logger.error(MqConstant.vehicleAlarmInformation + "创建失败");
                    return new ResponseMes(false, "999", MqConstant.vehicleLocationReissue + "创建失败");
                }
            }
            //endregion

            stream.map(s -> {
                VehicleLocationReissue vehicleLocationReissue = JSON.parseObject(s.value(), VehicleLocationReissue.class);
                System.out.println(vehicleLocationReissue);
                return JSON.parseObject(s.value(), VehicleLocationReissue.class);
            }).foreachRDD(rdd -> {
                List<VehicleLocationReissue> collect = rdd.collect();

                if (!collect.isEmpty()) {
                    boolean flag = hBaseRepository.insertManyColumnRecords(MqConstant.vehicleLocationReissue,collect );
                    if (!flag)logger.error("保存Hbase数据失败");
                }
            });
            jssc.start();
            jssc.awaitTermination();
            jssc.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseMes(true, null);
    }
    //endregion

    //region  车辆实时位置
    public ResponseMes getVehicleRealTimeLocation() {
        // 构建SparkStreaming上下文
        SparkConf conf = new SparkConf().setAppName("reissue")
                .setMaster("local[2]")
                //序列化
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        // 每隔5秒钟，sparkStreaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 构建kafka参数map
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaConfig.getServers());
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", kafkaConfig.getGroupid());
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);


        // 构建topic set
        Collection<String> topics = new HashSet<>();
        topics.add(MqConstant.vehicleRealTimeLocation);

        try {
            // 获取kafka的数据
            final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
            );

            //region 创建Hbase的相应表
            boolean isexist = hBaseRepository.isexist(MqConstant.vehicleRealTimeLocation);
            if (!isexist) {
                List<String> list = new ArrayList<>();
                list.add("common");
                list.add("location");
                int oneTable = hBaseRepository.createOneTable(MqConstant.vehicleRealTimeLocation, list);
                if (oneTable != 1 && oneTable != 2) {
                    logger.error(MqConstant.vehicleRealTimeLocation + "创建失败");
                    return new ResponseMes(false, "999", MqConstant.vehicleRealTimeLocation + "创建失败");
                }
            }
            //endregion

            stream.map(s -> {
                VehicleLocation vehicleLocation = JSON.parseObject(s.value(), VehicleLocation.class);
                System.out.println(vehicleLocation);
                return JSON.parseObject(s.value(), VehicleLocation.class);
            }).foreachRDD(rdd -> {
                List<VehicleLocation> collect = rdd.collect();

                if (!collect.isEmpty()) {
                    boolean flag = hBaseRepository.insertManyColumnRecords(MqConstant.vehicleRealTimeLocation,collect );
                    if (!flag){
                        logger.error("保存Hbase数据失败");
                    }
                }
            });
            jssc.start();
            jssc.awaitTermination();
            jssc.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseMes(true, null);
    }
    //endregion

    //region  平台车辆注册请求
    public ResponseMes getVehicleRegister() {
        // 构建SparkStreaming上下文
        SparkConf conf = new SparkConf().setAppName("register")
                .setMaster("local[2]")
                //序列化
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        // 每隔5秒钟，sparkStreaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 构建kafka参数map
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaConfig.getServers());
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", kafkaConfig.getGroupid());
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);


        // 构建topic set
        Collection<String> topics = new HashSet<>();
        topics.add(MqConstant.vehicleRegister);

        try {
            // 获取kafka的数据
            final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
            );

            //region 创建Hbase的相应表
            boolean isexist = hBaseRepository.isexist(MqConstant.vehicleRegister);
            if (!isexist) {
                List<String> list = new ArrayList<>();
                list.add("common");
                list.add("register");
                int oneTable = hBaseRepository.createOneTable(MqConstant.vehicleRegister, list);
                if (oneTable != 1 && oneTable != 2) {
                    logger.error(MqConstant.vehicleRegister + "创建失败");
                    return new ResponseMes(false, "999", MqConstant.vehicleRegister + "创建失败");
                }
            }
            //endregion

            stream.map(s -> {
                VehicleRegister vehicleRegister = JSON.parseObject(s.value(), VehicleRegister.class);
                System.out.println(vehicleRegister);
                return JSON.parseObject(s.value(), VehicleRegister.class);
            }).foreachRDD(rdd -> {
                List<VehicleRegister> collect = rdd.collect();

                if (!collect.isEmpty()) {
                    boolean flag = hBaseRepository.insertManyColumnRecords(MqConstant.vehicleRegister,collect );
                    if (!flag)logger.error("保存Hbase数据失败");
                }
            });
            jssc.start();
            jssc.awaitTermination();
            jssc.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseMes(true, null);
    }
    //endregion

    //region  车辆ADAS报警信息
    public ResponseMes getVehicleAdasInfo() {
        // 构建SparkStreaming上下文
        SparkConf conf = new SparkConf().setAppName("adas")
                .setMaster("local[2]")
                //序列化
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        // 每隔5秒钟，sparkStreaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 构建kafka参数map
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaConfig.getServers());
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", kafkaConfig.getGroupid());
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);


        // 构建topic set
        Collection<String> topics = new HashSet<>();
        topics.add(MqConstant.vehicleAdasInfo);

        try {
            // 获取kafka的数据
            final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
            );

            //region 创建Hbase的相应表
            boolean isexist = hBaseRepository.isexist(MqConstant.vehicleAdasInfo);
            if (!isexist) {
                List<String> list = new ArrayList<>();
                list.add("adas");
                int oneTable = hBaseRepository.createOneTable(MqConstant.vehicleAdasInfo, list);
                if (oneTable != 1 && oneTable != 2) {
                    logger.error(MqConstant.vehicleAdasInfo + "创建失败");
                    return new ResponseMes(false, "999", MqConstant.vehicleAdasInfo + "创建失败");
                }
            }
            //endregion

            stream.map(s -> {
                List<Adasinfo> adasinfo = JSON.parseArray(s.value(), Adasinfo.class);
                System.out.println(adasinfo);
                return adasinfo;
            }).foreachRDD(rdd -> {
                List<List<Adasinfo>> collect = rdd.collect();
                List<Adasinfo> list = new ArrayList<>();
                for (List<Adasinfo> adasinfos : collect) {
                    list.addAll(adasinfos);
                }

                if (!collect.isEmpty()) {
                    boolean flag = hBaseRepository.insertManyColumnRecords(MqConstant.vehicleAdasInfo,list );
                    if (!flag)logger.error("保存Hbase数据失败");
                }
            });
            jssc.start();
            jssc.awaitTermination();
            jssc.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseMes(true, null);
    }
    //endregion

    //region  车辆DMS报警信息
    public ResponseMes getVehicleDms() {
        // 构建SparkStreaming上下文
        SparkConf conf = new SparkConf().setAppName("dms")
                .setMaster("local[2]")
                //序列化
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        // 每隔5秒钟，sparkStreaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 构建kafka参数map
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaConfig.getServers());
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", kafkaConfig.getGroupid());
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);


        // 构建topic set
        Collection<String> topics = new HashSet<>();
        topics.add(MqConstant.vehicleDmsInfo);

        try {
            // 获取kafka的数据
            final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
            );

            //region 创建Hbase的相应表
            boolean isexist = hBaseRepository.isexist(MqConstant.vehicleDmsInfo);
            if (!isexist) {
                List<String> list = new ArrayList<>();
                list.add("dms");
                int oneTable = hBaseRepository.createOneTable(MqConstant.vehicleDmsInfo, list);
                if (oneTable != 1 && oneTable != 2) {
                    logger.error(MqConstant.vehicleDmsInfo + "创建失败");
                    return new ResponseMes(false, "999", MqConstant.vehicleDmsInfo + "创建失败");
                }
            }
            //endregion
            stream.map(s ->{
                //转换成实体类
                List<DmsInfo> dmsInfoList = JSON.parseArray(s.value(), DmsInfo.class);
                return dmsInfoList;
            }).foreachRDD(rdd -> {
                List<List<DmsInfo>> collect = rdd.collect();
                List<DmsInfo> list =new ArrayList<>();
                for (List<DmsInfo> dmsInfoList : collect) {
                    list.addAll(dmsInfoList);
                }
                if (!collect.isEmpty()) {
                    boolean flag = hBaseRepository.insertManyColumnRecords(MqConstant.vehicleDmsInfo,list );
                    if (!flag)logger.error("保存Hbase数据失败");
                }
            });
            jssc.start();
            jssc.awaitTermination();
            jssc.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseMes(true, null);
    }
    //endregion

    //region  车辆DMS报警信息
    public ResponseMes getlogin() {
        // 构建SparkStreaming上下文
        SparkConf conf = new SparkConf().setAppName("dms")
                .setMaster("local[2]")
                //序列化
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        // 每隔5秒钟，sparkStreaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // 构建kafka参数map
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", kafkaConfig.getServers());
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", kafkaConfig.getGroupid());
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);


        // 构建topic set
        Collection<String> topics = new HashSet<>();
        topics.add(MqConstant.vehicleDmsInfo);

        try {
            // 获取kafka的数据
            final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
            );

            //region 创建Hbase的相应表
//            boolean isexist = hBaseRepository.isexist(MqConstant.vehicleDmsInfo);
//            if (!isexist) {
//                List<String> list = new ArrayList<>();
//                list.add("dms");
//                int oneTable = hBaseRepository.createOneTable(MqConstant.vehicleAdasInfo, list);
//                if (oneTable != 1 && oneTable != 2) {
//                    logger.error(MqConstant.vehicleDmsInfo + "创建失败");
//                    return new ResponseMes(false, "999", MqConstant.vehicleAdasInfo + "创建失败");
//                }
//            }
            //endregion
            stream.map(s ->{
                //转换成实体类
//                DmsInfo dmsInfoList = JSON.parseObject(s.value(), DmsInfo.class);
                return s.value();
            }).foreachRDD(rdd -> {
//                List<DmsInfo> collect = rdd.collect();
                rdd.collect();
//                if (!collect.isEmpty()) {
//                    boolean flag = hBaseRepository.insertManyColumnRecords(MqConstant.vehicleDmsInfo,collect );
//                    if (!flag)logger.error("保存Hbase数据失败");
//                }
            });
            jssc.start();
            jssc.awaitTermination();
            jssc.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return new ResponseMes(true, null);
    }
    //endregion
}


