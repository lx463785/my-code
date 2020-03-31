package com.ymhx.dataplatform.kafka;

import com.ymhx.dataplatform.kafka.config.HbaseConfigMessage;
import com.ymhx.dataplatform.kafka.untils.ADASEnum;
import com.ymhx.dataplatform.kafka.untils.DateUtils;
import com.ymhx.dataplatform.kafka.untils.JdbcUtils;
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
import org.apache.spark.api.java.function.Function;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

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
        for (String terminalId : list) {
            //倒序
            String reverseId = StringUtils.reverse((terminalId));
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

           //查询终端id为terminalId的车辆信息
            List<String> run = new JdbcUtils().getTerminalData(terminalId);
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
                    String terminalId = Bytes.toString(result.getValue("alarm".getBytes(), "terminalId".getBytes()));
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

                    return new Tuple2<String, Double>(terminalId + "_" + alarmType, marking);
                }
            }).reduceByKey(new Function2<Double, Double, Double>() {
                @Override
                public Double call(Double aDouble, Double aDouble2) throws Exception {
                    return aDouble + aDouble2;
                }
            });

            stringDoubleJavaPairRDD .mapToPair(new PairFunction<Tuple2<String, Double>, String, String>() {
                @Override
                public Tuple2<String, String> call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
                    List<String> asList = Arrays.asList(stringDoubleTuple2._1.split("_"));
                    String mes = asList.get(1) + "_" + stringDoubleTuple2._2;
                    String s = asList.get(0);
                   String sd= asList.get(1) + "_" + stringDoubleTuple2._2;

                    return new Tuple2<>(asList.get(0), asList.get(1) + "_" + stringDoubleTuple2._2);
                }
            }) .reduceByKey(new Function2<String, String, String>() {
                @Override
                public String call(String s, String s2) throws Exception {
                    Double making =0.00;
                    if (s2.contains("_")){
                        List<String> values = Arrays.asList(s2.split("_"));
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
                        making+=Double.parseDouble(s2);
                    }

                    if (s.contains("_")){
                        List<String> values1 = Arrays.asList(s.split("_"));
                        int alarmtype1 = Integer.parseInt(values1.get(0));
                        if (alarmtype1==ADASEnum.FCW.getVaule()){
                            making+= Double.parseDouble( values1.get(1))*Double.valueOf(configlist.get(5));
                        }else if (alarmtype1==ADASEnum.LDW.getVaule()||alarmtype1==ADASEnum.LDWR.getVaule()){
                            making+= Double.parseDouble( values1.get(1))*Double.valueOf(configlist.get(9));
                        }else if (alarmtype1==ADASEnum.HMW.getVaule()){
                            making+= Double.parseDouble( values1.get(1))*Double.valueOf(configlist.get(14));
                        }else {
                            making+= Double.parseDouble( values1.get(1));
                        }
                    }else {
                        making+=Double.parseDouble(s);
                    }
                    return String .valueOf(making );
                }
            }).foreach(new VoidFunction<Tuple2<String, String>>() {
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

                    int terminalid = Integer.parseInt(terminalId);
                    DateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    //查询是否有该数据
                    String querysql = "SELECT * from tb_vehicle_report WHERE terminal_id=? AND create_time>? AND create_time<=? ";
                    querysql= String.format(querysql, terminalid, DateUtils.getBeforeOneDay().get("startTime"), DateUtils.getBeforeOneDay().get("endTime"));
                    List<String> query = new JdbcUtils().query(querysql,terminalid,DateUtils.getcurrentTime().get("startTime"),DateUtils.getcurrentTime().get("endTime"));
                    if (query.size()==0){
                        //不存在进行新增操作
                        String sql = "insert into tb_vehicle_report  (terminal_id,warn_risk,create_time) values (?,?,?)";
                        DecimalFormat df = new DecimalFormat("0.00");
                        df.setRoundingMode(RoundingMode.HALF_UP);
                        String format = df.format(making);
                        new JdbcUtils().save(sql, Integer.parseInt(terminalId),format,dateFmt.format(new Date()));
                    }else {
                        //以前的risk
                        double oldmaking = Double.parseDouble(query.get(1));
                        String sql = "update tb_vehicle_report set  warn_risk=?,create_time=?  where id=? ";
                        DecimalFormat df = new DecimalFormat("0.00");
                        df.setRoundingMode(RoundingMode.HALF_UP);
                        making=oldmaking+making;
                        String format = df.format(making);
                        new JdbcUtils().save(sql, Integer.parseInt(query.get(0)), format, dateFmt.format(new Date()));
                    }
                }
            });
        }

    }
}
