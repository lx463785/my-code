//package com.ymhx.dataplatform.kafka;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.util.Base64;
//import com.ymhx.dataplatform.kafka.config.KafkaConfigMessage;
//import com.ymhx.dataplatform.kafka.domain.MqConstant;
//import com.ymhx.dataplatform.kafka.domain.ResponseMes;
//import com.ymhx.dataplatform.kafka.pojo.Adasinfo;
//import com.ymhx.dataplatform.kafka.pojo.VehicleLocationReissue;
//import com.ymhx.dataplatform.kafka.pojo.VehicleReportAlarm;
//import com.ymhx.dataplatform.kafka.untils.ConnectionPool;
//import com.ymhx.dataplatform.kafka.untils.HBaseRepository;
//import com.ymhx.dataplatform.kafka.untils.JdbcUtils;
//import org.apache.commons.lang.StringUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.filter.*;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
//import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
//import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.common.serialization.StringDeserializer;
//import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.streaming.Durations;
//import org.apache.spark.streaming.api.java.JavaDStream;
//import org.apache.spark.streaming.api.java.JavaInputDStream;
//import org.apache.spark.streaming.api.java.JavaPairDStream;
//import org.apache.spark.streaming.api.java.JavaStreamingContext;
//import org.apache.spark.streaming.kafka010.ConsumerStrategies;
//import org.apache.spark.streaming.kafka010.KafkaUtils;
//import org.apache.spark.streaming.kafka010.LocationStrategies;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.stereotype.Component;
//import org.springframework.util.Base64Utils;
//import scala.Tuple2;
//
//import java.io.IOException;
//import java.io.Serializable;
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.text.DateFormat;
//import java.text.SimpleDateFormat;
//import java.util.*;
//
//import static org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.convertScanToString;
//
//@Component
//public class StatictalVehicheInformation implements Serializable {
//
//
////    @Autowired
////    private KafkaConfigMessage kafkaConfig;
////    @Autowired
////    private HBaseRepository hBaseRepository;
////    @Autowired
////    private JdbcTemplate jdbcTemplate;
//
//    private Logger logger = LoggerFactory.getLogger(this.getClass());
//
//    //region  统计不同类型的报警信息
//    public ResponseMes getCountAlarmInformation() {
//        // 构建SparkStreaming上下文
//        SparkConf conf = new SparkConf().setAppName("count")
//                .setMaster("local[2]")
//                //序列化
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//
//        // 每隔5秒钟，sparkStreaming作业就会收集最近5秒内的数据源接收过来的数据
//        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
//
//        // 构建kafka参数map
//        Map<String, Object> kafkaParams = new HashMap<>();
//        kafkaParams.put("bootstrap.servers", "192.168.0.95:9092");
//        kafkaParams.put("key.deserializer", StringDeserializer.class);
//        kafkaParams.put("value.deserializer", StringDeserializer.class);
//        kafkaParams.put("group.id", "count");
//        kafkaParams.put("auto.offset.reset", "latest");
//        kafkaParams.put("enable.auto.commit", false);
//
//
//        // 构建topic set
//        Collection<String> topics = new HashSet<>();
//        topics.add(MqConstant.vehicleAlarmInformation);
//
//        try {
//            // 获取kafka的数据
//            final JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
//                    jssc,
//                    LocationStrategies.PreferConsistent(),
//                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
//            );
//
//            JavaDStream<VehicleReportAlarm> map = stream.map(s -> {
//                VehicleReportAlarm vehicleReportAlarm = JSON.parseObject(s.value(), VehicleReportAlarm.class);
//                System.out.println(vehicleReportAlarm);
//                return vehicleReportAlarm;
//            });
//           map.mapToPair(new PairFunction<VehicleReportAlarm, String, String>() {
//                @Override
//                public Tuple2<String, String> call(VehicleReportAlarm vehicleReportAlarm) throws Exception {
//
//                    return new Tuple2<>(vehicleReportAlarm.getVehicleNo(), JSON.toJSONString(vehicleReportAlarm));
//                }
//            }).reduceByKey(new Function2<String, String, String>() {
//                @Override
//                public String call(String s, String s2) throws Exception {
//
//                    return s + "_" + s2;
//                }
//            }).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
//                @Override
//                public Tuple2<String, String> call(Tuple2<String, String> datastring) throws Exception {
//                    //将统计的数量存入mysql
//                    List<String> list = Arrays.asList(datastring._2.split("_"));
//                    list.sort(new Comparator<String>() {
//                        @Override
//                        public int compare(String o1, String o2) {
//                            VehicleReportAlarm vehicleReportAlarm = JSON.parseObject(o1, VehicleReportAlarm.class);
//                            VehicleReportAlarm vehicleReportAlarm1 = JSON.parseObject(o2, VehicleReportAlarm.class);
//                            int i = vehicleReportAlarm.getData().getWarnTime().compareTo(vehicleReportAlarm1.getData().getWarnTime());
//                            if (i > 0) {
//                                return 1;
//                            }
//                            return 0;
//                        }
//                    });
//                    //保存统计数据（数量和时间）
//                    VehicleReportAlarm firstvehicleReportAlarm = JSON.parseObject(list.get(0), VehicleReportAlarm.class);
//                    VehicleReportAlarm lastvehicleReportAlarm = JSON.parseObject(list.get(list.size() - 1), VehicleReportAlarm.class);
//                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//                    String startTime = dateFormat.format(firstvehicleReportAlarm.getData().getWarnTime());
//                    String endTime = dateFormat.format(lastvehicleReportAlarm.getData().getWarnTime());
//                    String sql = "insert into tb_vehicle_count  (vehicle_no,alarm_num,startTime,endTime) values ('%s',%s,'%s','%s')";
//                    sql = String.format(sql, firstvehicleReportAlarm.getVehicleNo(), list.size(),startTime,endTime);
//                    new JdbcUtils().run(sql);
//                    return new Tuple2<String, String>(firstvehicleReportAlarm.getVehicleNo(),datastring._2);
//                }
//            }).toJavaDStream().flatMap(new FlatMapFunction<Tuple2<String, String>, String>() {
//                @Override
//                public Iterator<String> call(Tuple2<String, String> stringTuple2) throws Exception {
//                    List<String> list = Arrays.asList(stringTuple2._2.split("_"));
//
//                    return list.iterator();
//                }
//            }).mapToPair(new PairFunction<String, String, Integer>() {
//                @Override
//                public Tuple2<String, Integer> call(String s) throws Exception {
//                    VehicleReportAlarm vehicleReportAlarm = JSON.parseObject(s, VehicleReportAlarm.class);
//
//                    return new Tuple2<>(vehicleReportAlarm.getVehicleNo()+"_"+vehicleReportAlarm.getData().getWarnType(),1);
//                }
//            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//                @Override
//                public Integer call(Integer integer, Integer integer2) throws Exception {
//                    return integer+integer2;
//                }
//            }).mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
//               @Override
//               public Tuple2<String, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                   List<String> list = Arrays.asList(stringIntegerTuple2._1.split("_"));
//
//                   return new Tuple2<>(list.get(0),list.get(1)+":"+stringIntegerTuple2._2);
//               }
//           }).reduceByKey(new Function2<String, String, String>() {
//               @Override
//               public String call(String s, String s2) throws Exception {
//
//                   return s+","+s2;
//               }
//           }).foreachRDD(rdd->{
//               List<Tuple2<String, String>> collect = rdd.collect();
//               for (Tuple2<String, String> stringStringTuple2 : collect) {
//                   String sql = " UPDATE   tb_vehicle_count SET  alarm_type ='%s' where  vehicle_no = '%s'  ORDER BY id LIMIT 1";
//                   sql = String.format(sql, stringStringTuple2._2, stringStringTuple2._1);
//                   new JdbcUtils().run(sql);
//               }
//           });
//
////            stream.map(s -> {
////                VehicleReportAlarm vehicleReportAlarm = JSON.parseObject(s.value(), VehicleReportAlarm.class);
////                System.out.println(vehicleReportAlarm);
////                return vehicleReportAlarm.vehicleNo;
////            }).mapToPair(
////                new PairFunction<String, String, Integer>() {
////
////                @Override
////                public Tuple2<String, Integer> call(String s) {
////
////                    return new Tuple2<>(s, 1);
////                }
////            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
////                @Override
////                public Integer call(Integer integer, Integer integer2) throws Exception {
////                    return integer2+integer;
////                }
////            }).foreachRDD(rdd->{
////                if (!rdd.isEmpty()){
////                    Connection connection = ConnectionPool.getConnection();
////
////                }
//////                        if(rdd.isEmpty){
//////                            rdd.foreachPartition(eachPartition => {
//////                                    val conn = ConnectionPool.getConnection();
//////                            eachPartition.foreach(record => {
//////                                    val sql = "insert into streaming(item,count) values('" + record._1 + "'," + record._2 + ")"
//////                                    val stmt = conn.createStatement
//////                                    stmt.executeUpdate(sql)
//////                            })
//////                            ConnectionPool.returnConnection(conn)
//////                Map<String, Integer> stringIntegerMap = rdd.collectAsMap();
////
////
////            });
//
//            jssc.start();
//            jssc.awaitTermination();
//            jssc.close();
//
//        } catch (Exception e) {
//            e.printStackTrace();
//            return  new ResponseMes(false,"999",e.getMessage());
//        }
//        return new ResponseMes(true, null);
//    }
//    //endregion
//
//    public void getMes() throws IOException, SQLException {
//        SparkConf conf = new SparkConf().setAppName("count")
//                .setMaster("local[2]")
//                //序列化
//                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//
//        JavaSparkContext context = new JavaSparkContext(conf);
//        //hbase配置
//        Configuration hconf = HBaseConfiguration.create();
//        hconf.set("hbase.zookeeper.quorum", "192.168.0.95:2181,192.168.0.46:2181,192.168.0.202:2181");
//        hconf.set("hbase.zookeeper.property.clientPort", "2181");
//        hconf.set(TableInputFormat.INPUT_TABLE, "vehicle_alarm_adas");
//        Scan scan = new Scan();
//        //获取所有车辆的terminal_id
//        String sql = "SELECT ve.TerminalID FROM vehicle_group gp LEFT JOIN vehicle ve ON gp.`\uFEFFID`=ve.GroupID WHERE gp.SuperiorID='100118'";
//        List<Integer> run = new JdbcUtils().run(sql);
//        for (Integer terminalId : run) {
//            //倒序
//            String sterminalId =StringUtils.reverse((""+ terminalId));
//
//        }
//        hconf.set(TableInputFormat.SCAN,  convertScanToString(scan));
//        JavaPairRDD<ImmutableBytesWritable, Result> javaPairRDD = context.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
//
//        JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = javaPairRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
//                Result result = immutableBytesWritableResultTuple2._2;
//                System.out.println("---------------------");
////                String s = new String(immutableBytesWritableResultTuple2._2().getValue("gps".getBytes(), "terminalId".getBytes()));
////                System.out.println(s);
//                System.out.println("--------------------");
//                return null;
//            }
//        });
//        long count = stringIntegerJavaPairRDD.count();
//        System.out.println("---------------------");
//        System.out.println(count);
//    }
//}
