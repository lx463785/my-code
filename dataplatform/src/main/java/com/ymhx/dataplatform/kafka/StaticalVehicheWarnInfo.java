package com.ymhx.dataplatform.kafka;

import com.google.common.collect.Lists;
import com.ymhx.dataplatform.kafka.config.HbaseConfigMessage;
import com.ymhx.dataplatform.kafka.untils.ADASEnum;
import com.ymhx.dataplatform.kafka.untils.DateUtils;
import com.ymhx.dataplatform.kafka.untils.JdbcUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Function;

import static org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.convertScanToString;

@Component
public class StaticalVehicheWarnInfo implements Serializable {




    /**
     * 每个车队的风险系数
     */
    public void getWarnCoefficient() throws IOException, SQLException, ParseException {
        SparkConf conf = new SparkConf().setAppName("warn")
                .setMaster("local[2]")
                //序列化
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaSparkContext context = new JavaSparkContext(conf);

        //查询配置信息
        List<String>  configlist = new JdbcUtils().getconfiglist();

        //获取所有车辆的terminal_id
        List<String> list = new JdbcUtils().getterminalID();
        for (String vehicleid : list) {
            //查询终端id为terminalId的车辆信息
            List<String> run = new JdbcUtils().getTerminalData(vehicleid);
            //倒序
            String reverseId = StringUtils.reverse((run.get(6)));

            //hbase配置
            Configuration hconf = HBaseConfiguration.create();
            hconf.set("hbase.zookeeper.quorum","192.168.0.95:2181,192.168.0.46:2181,192.168.0.202:2181");
            hconf.set("hbase.zookeeper.property.clientPort", "2181");
            hconf.set(TableInputFormat.INPUT_TABLE, "vehicle_alarm_adas");

            Scan scan = new Scan();
            scan.setStartRow(String.format("%s%s", reverseId, DateUtils.getBeforeOneDay().get("startTime")).getBytes());
            scan.setStopRow(String.format("%s%s", reverseId, DateUtils.getBeforeOneDay().get("endTime")).getBytes());
            hconf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
            hconf.set(TableInputFormat.SCAN_ROW_START,String.format("%s%s", reverseId,  DateUtils.getBeforeOneDay().get("startTime")));
            hconf.set(TableInputFormat.SCAN_ROW_STOP,String.format("%s%s", reverseId, DateUtils.getBeforeOneDay().get("endTime")));


            //设置标识符
            //获取符合查询的hbase相应信息
            JavaPairRDD<ImmutableBytesWritable, Result> javaPairRDD = context.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            long count = javaPairRDD.count();
            JavaPairRDD<String, Double> stringDoubleJavaPairRDD = javaPairRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Double>() {
                @Override
                public Tuple2<String, Double> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                    Result result = immutableBytesWritableResultTuple2._2();
                    //获取每个车辆的报警类型
                    Integer alarmType = Integer.valueOf(Bytes.toString(result.getValue("alarm".getBytes(), "alarmType".getBytes())));
                    //获取每个车辆的终端ID
                    String vehicleId = Bytes.toString(result.getValue("alarm".getBytes(), "vehicleId".getBytes()));
                    //获取每个车的速度进行判断
                    Double speed = Double.valueOf(Bytes.toString(result.getValue("alarm".getBytes(), "speed".getBytes())));
                    Double marking = 0.00;

                    if (alarmType == ADASEnum.FCW.getVaule()) {  //.....前碰撞
                        //判断速度为低速还是高速
                        if (speed >= Double.valueOf(configlist.get(15))) {
                            marking += 1 * Double.valueOf(configlist.get(3));
                        } else if (speed < Double.valueOf(configlist.get(15)) && speed >= Double.valueOf(configlist.get(16))) {
                            marking += 1 * Double.valueOf(configlist.get(4));
                        } else {
                            marking = 0.00;
                        }
                    } else if (alarmType == ADASEnum.UFCW.getVaule()) { //......低速碰撞
                        if (speed >= Double.valueOf(configlist.get(17))) {
                            marking += 1 * Double.valueOf(configlist.get(6));
                        }
                    } else if (alarmType == ADASEnum.LDW.getVaule() || alarmType == ADASEnum.LDWR.getVaule()) { //......车道偏移
                        if (speed >= Double.valueOf(configlist.get(18))) {
                            marking += 1 * Double.valueOf(configlist.get(7));
                        } else {
                            marking += 1 * Double.valueOf(configlist.get(8));
                        }
                    } else if (alarmType == ADASEnum.PCW.getVaule()) {  //.....行人碰撞
                        marking += 1 * Double.valueOf(configlist.get(10));
                    } else if (alarmType == ADASEnum.HMW.getVaule()) {  //.....车距检测
                        if (speed >= Double.valueOf(configlist.get(20))) {
                            marking += 1 * Double.valueOf(configlist.get(11));
                        } else {
                            marking += 1 * Double.valueOf(configlist.get(12));
                        }
                    } else if (alarmType == ADASEnum.TSR.getVaule()) {  //.....超速
                        if (speed >= Double.valueOf(configlist.get(19))) {
                            marking += 1 * Double.valueOf(configlist.get(14));
                        } else {
                            marking = marking;
                        }
                    }

                    return new Tuple2<String, Double>(vehicleId + "_" + alarmType, marking);
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double aDouble, Double aDouble2) throws Exception {
                    return aDouble + aDouble2;
                }
            });

            JavaPairRDD<String, String> pairRDD = stringDoubleJavaPairRDD.mapToPair(new PairFunction<Tuple2<String, Double>, String, String>() {
                @Override
                public Tuple2<String, String> call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                    List<String> asList = Arrays.asList(stringDoubleTuple2._1.split("_"));
                    String mes = asList.get(1) + "_" + stringDoubleTuple2._2;
                    String s = asList.get(0);
                    String sd = asList.get(1) + "_" + stringDoubleTuple2._2;

                    return new Tuple2<>(asList.get(0), asList.get(1) + "_" + stringDoubleTuple2._2);
                }
            }).reduceByKey(new Function2<String, String, String>() {
                @Override
                public String call(String s, String s2) throws Exception {
                    Double making = 0.00;
                    if (s2.contains("_")) {
                        List<String> values = Arrays.asList(s2.split("_"));
                        int alarmtype = Integer.parseInt(values.get(0));
                        //对前碰撞 车道偏移 车距检测省基数
                        if (alarmtype == ADASEnum.FCW.getVaule()) {
                            making += Double.parseDouble(values.get(1)) * Double.valueOf(configlist.get(5));
                        } else if (alarmtype == ADASEnum.LDW.getVaule() || alarmtype == ADASEnum.LDWR.getVaule()) {
                            making += Double.parseDouble(values.get(1)) * Double.valueOf(configlist.get(9));
                        } else if (alarmtype == ADASEnum.HMW.getVaule()) {
                            making += Double.parseDouble(values.get(1)) * Double.valueOf(configlist.get(14));
                        } else {
                            making += Double.parseDouble(values.get(1));
                        }
                    } else {
                        making += Double.parseDouble(s2);
                    }

                    if (s.contains("_")) {
                        List<String> values1 = Arrays.asList(s.split("_"));
                        int alarmtype1 = Integer.parseInt(values1.get(0));
                        if (alarmtype1 == ADASEnum.FCW.getVaule()) {
                            making += Double.parseDouble(values1.get(1)) * Double.valueOf(configlist.get(5));
                        } else if (alarmtype1 == ADASEnum.LDW.getVaule() || alarmtype1 == ADASEnum.LDWR.getVaule()) {
                            making += Double.parseDouble(values1.get(1)) * Double.valueOf(configlist.get(9));
                        } else if (alarmtype1 == ADASEnum.HMW.getVaule()) {
                            making += Double.parseDouble(values1.get(1)) * Double.valueOf(configlist.get(14));
                        } else {
                            making += Double.parseDouble(values1.get(1));
                        }
                    } else {
                        making += Double.parseDouble(s);
                    }
                    return String.valueOf(making);
                }
            });

            pairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
                @Override
                public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                    //对只有一种类型的进行判断
                    Double making=0.00;
                    if (stringStringTuple2._2.contains("_")){
                        List<String> values = Arrays.asList(stringStringTuple2._2.split("_"));
                        int alarmtype = Integer.parseInt(values.get(0));
                        //对前碰撞 车道偏移 车距检测省基数
                        if (alarmtype==ADASEnum.FCW.getVaule()){
                            making+= Double.parseDouble( values.get(1))*Double.valueOf(configlist.get(5));
                        }else if (alarmtype==ADASEnum.LDW.getVaule()||alarmtype==ADASEnum.LDWR.getVaule()){
                            making+= Double.parseDouble( values.get(1))*Double.valueOf(configlist.get(9));
                        }else if (alarmtype==ADASEnum.HMW.getVaule()){
                            making+= Double.parseDouble( values.get(1))*Double.valueOf(configlist.get(14));
                        }else {
                            making+= Double.parseDouble( values.get(1));
                        }
                    }else {
                        //向mysql插入统计数据
                        making = Double.parseDouble(stringStringTuple2._2);
                    }

                    int vehicleId = Integer.parseInt(vehicleid);
                    DateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    //查询是否有该数据
                    String querysql = "SELECT * from tb_vehicle_report WHERE vehicle_id=? AND create_time>? AND create_time<=? ";
                    querysql= String.format(querysql, vehicleId, DateUtils.getBeforeOneDay().get("startTime"), DateUtils.getBeforeOneDay().get("endTime"));
                    List<String> query = new JdbcUtils().query(querysql,vehicleId,DateUtils.getcurrentTime().get("startTime"),DateUtils.getcurrentTime().get("endTime"));
                    if (query.size()==0){
                        //不存在进行新增操作
                        String sql = "insert into tb_vehicle_report  (terminal_id,vehicle_id,number_plate,superior_id,warn_risk,create_time,record_date) values (?,?,?,?,?,?,?)";
                        DecimalFormat df = new DecimalFormat("0.00");
                        df.setRoundingMode(RoundingMode.HALF_UP);
                        String format = df.format(making);
                        new JdbcUtils().save(sql, Integer.parseInt(run.get(6)),vehicleId,run.get(1),Integer.parseInt(run.get(12)),
                                format,dateFmt.format(new Date()),dateFmt.format(DateUtils.getBeforeOneDay().get("startTime")));
                    }else {
                        //以前的risk
                        double oldmaking = Double.parseDouble(query.get(1));
                        String sql = "update tb_vehicle_report set  warn_risk=?,create_time=?  where id=? ";
                        DecimalFormat df = new DecimalFormat("0.00");
                        df.setRoundingMode(RoundingMode.HALF_UP);
                        making=oldmaking+making;
                        String format = df.format(making);
                        new JdbcUtils().update(sql, Integer.parseInt(query.get(0)), format, dateFmt.format(new Date()));
                    }
                }
            });

            //合并数据
            JavaPairRDD<String, String> newrdd = javaPairRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                    Result result = immutableBytesWritableResultTuple2._2();
                    //获取每个车辆的报警类型
                    Integer alarmType = Integer.valueOf(Bytes.toString(result.getValue("alarm".getBytes(), "alarmType".getBytes())));
                    //获取每个车辆的终端ID
                    String vehicleId = Bytes.toString(result.getValue("alarm".getBytes(), "vehicleId".getBytes()));
                    return new Tuple2<>(vehicleId +"_"+ alarmType, 1);
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer integer, Integer integer2) throws Exception {

                    return integer + integer2;
                }
            }).mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
                @Override
                public Tuple2<String, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                    List<String> newlist = Arrays.asList(stringIntegerTuple2._1.split("_"));
                    return new Tuple2<>(newlist.get(0), newlist.get(1) + "_" + stringIntegerTuple2._2);
                }
            });

            JavaPairRDD<String, Tuple2<String, String>> join = pairRDD.join(newrdd);
            join.foreach(new VoidFunction<Tuple2<String, Tuple2<String, String>>>() {
                @Override
                public void call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
                    System.out.println(stringTuple2Tuple2._2._1);
                    System.out.println(stringTuple2Tuple2._1);
                    List<String> newlist = Arrays.asList(stringTuple2Tuple2._2._2.split("_"));
                    int type = Integer.parseInt(newlist.get(0));
                    int counts = Integer.parseInt(newlist.get(1));

                    //插入报警信息
                    String sql ="update tb_vehicle_report set %s=?  where vehicle_id=? and create_time>? and create_time<? ";
                    String name = "";
                    if (ADASEnum.FCW.getVaule()==type){  //前碰撞
                        name="fwc";
                    }else if (ADASEnum.UFCW.getVaule()==type){//低速前碰撞
                        name="ufcw";
                    }else if (ADASEnum.LDW.getVaule()==type){
                        name="ldw";
                    }else if (ADASEnum.LDWR.getVaule()==type){
                        name="rdw";
                    }else if (ADASEnum.HMW.getVaule()==type){
                        name="hmw";
                    }else if (ADASEnum.PCW.getVaule()==type){
                        name="pcw";
                    }else if (ADASEnum.FFW.getVaule()==type){
                        name="failure";
                    }else if (ADASEnum.TSR.getVaule()==type){
                        name="transfinite";
                    }
                    if (StringUtils.isNotBlank(name)) {
                        sql = String.format(sql, name);
                        new JdbcUtils().save(sql,String.valueOf(counts),vehicleid, DateUtils.getcurrentTime().get("startTime"), DateUtils.getcurrentTime().get("endTime"));
                    }
                }

            });

        }

    }

    /**
     * 计算里程
     *
     */
    public void  getMileageCount() throws SQLException, ParseException, IOException {
        SparkConf conf = new SparkConf().setAppName("warn")
                .setMaster("local[2]")
                //序列化
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        JavaSparkContext context = new JavaSparkContext(conf);


        //获取所有车辆的terminal_id
        List<String> list = new JdbcUtils().getterminalID();
        for (String vehicleid : list) {
            //倒序

            String reverseId = StringUtils.reverse((vehicleid));
            reverseId = String.format("%-14s", reverseId).replace(' ', '0');
            //hbase配置
            Configuration hconf = HBaseConfiguration.create();
            hconf.set("hbase.zookeeper.quorum","192.168.0.95:2181,192.168.0.46:2181,192.168.0.202:2181");
            hconf.set("hbase.zookeeper.property.clientPort", "2181");
            hconf.set(TableInputFormat.INPUT_TABLE, "vehicle_alarm_adas");

            Scan scan = new Scan();
            Long startTime = DateUtils.getBeforeOneDay().get("startTime");
            Long endTime = DateUtils.getBeforeOneDay().get("endTime");
            scan.setStartRow(String.format("%s%s", reverseId, startTime).getBytes());
            scan.setStopRow(String.format("%s%s", reverseId, endTime).getBytes());
            hconf.set(TableInputFormat.SCAN, TableMapReduceUtil.convertScanToString(scan));
            hconf.set(TableInputFormat.SCAN_ROW_START,String.format("%s%s", reverseId,  startTime));
            hconf.set(TableInputFormat.SCAN_ROW_STOP,String.format("%s%s", reverseId, endTime));

            //查询终端id为terminalId的车辆信息
            List<String> run = new JdbcUtils().getTerminalData(vehicleid);
            //设置标识符
            //获取符合查询的hbase相应信息
            JavaPairRDD<ImmutableBytesWritable, Result> javaPairRDD = context.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            long count = javaPairRDD.count();
            //统计公里数
            JavaPairRDD<String, Double> mileagerdd = javaPairRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, Integer, String>() {
                @Override
                public Tuple2<Integer, String> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                    Result result = immutableBytesWritableResultTuple2._2();
                    //获取每个车辆的终端ID
                    int terminalId = Bytes.toInt(result.getValue("alarm".getBytes(), "terminalId".getBytes()));
                    //获取每辆车的id
                    int vehicleId = Bytes.toInt(result.getValue("alarm".getBytes(), "vehicleId".getBytes()));
                    //获取车辆的公里数
                    Double mileage = Bytes.toDouble(result.getValue("alarm".getBytes(), "mileage".getBytes()));
                    //获取时间
                    String warningTime = Bytes.toString(result.getValue("alarm".getBytes(), "warningTime".getBytes()));

                    return new Tuple2<>(vehicleId, terminalId + "_" + warningTime + "_" + mileage);
                }
            }).groupByKey().sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer, Iterable<String>>, String, Double>() {
                @Override
                public Tuple2<String, Double> call(Tuple2<Integer, Iterable<String>> integerIterableTuple2) throws Exception {
                    Iterable<String> strings = integerIterableTuple2._2();
                    ArrayList<String> newArrayList = Lists.newArrayList(strings);
                    Collections.sort(newArrayList, new Comparator<String>() {
                        @Override
                        public int compare(String o1, String o2) {
                            List<String> newlist = Arrays.asList(o1.split("_"));
                            List<String> oldlist = Arrays.asList(o2.split("_"));
                            if (Integer.parseInt(newlist.get(0)) >= Integer.parseInt(oldlist.get(0))) {
                                return 0;
                            }
                            return 1;
                        }
                    });
                    Collections.sort(newArrayList, new Comparator<String>() {
                        @Override
                        public int compare(String o1, String o2) {
                            List<String> newlist = Arrays.asList(o1.split("_"));
                            List<String> oldlist = Arrays.asList(o2.split("_"));
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            try {
                                Date newdate = sdf.parse(newlist.get(1));
                                Date olddate = sdf.parse(oldlist.get(1));
                                if (newdate.getTime() >= olddate.getTime()) {
                                    return 0;
                                }
                            } catch (ParseException e) {
                                e.printStackTrace();
                            }

                            return 1;
                        }
                    });
                    //得到所有的值
                    List<Double> doubles = new ArrayList<>();
                    for (String s : newArrayList) {
                        List<String> list1 = Arrays.asList(s.split("_"));
                        double value = Double.parseDouble(list1.get(2));
                        doubles.add(value);
                    }
                    Double odd = 0.00;
                    for (int i = 0; i < doubles.size(); i++) {
                        if (i > 0) {
                            double value = doubles.get(i) - doubles.get(i - 1);
                            if (value >= 100) {
                                value = 0.00;
                            }
                            odd += value;
                        }

                    }
                    return new Tuple2<>(integerIterableTuple2._1.toString(), odd);
                }
            });
            mileagerdd.foreach(new VoidFunction<Tuple2<String, Double>>() {
                @Override
                public void call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                    DateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    //格式化日期
                    String start = dateFmt.format(startTime);
                    String end = dateFmt.format(endTime);
                    String sql ="update tb_vehicle_report set mileage=?  where vehicle_id=? and create_time>? and create_time<? ";
                    new JdbcUtils().save(sql, String.valueOf(stringDoubleTuple2._2),stringDoubleTuple2._1,  start, end);
                }
            });
        }
    }
}
