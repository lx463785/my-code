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
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.ParseException;
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

            JavaPairRDD<ImmutableBytesWritable, Result> javaPairRDD = context.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

            JavaPairRDD<String, Double> stringDoubleJavaPairRDD = javaPairRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                    Result result = immutableBytesWritableResultTuple2._2();
                    //获取每个车辆的报警类型
                    String alarmType = Bytes.toString(result.getValue("alarm".getBytes(), "alarmType".getBytes()));
                    //获取每个车辆的终端ID
                    String terminalId = Bytes.toString(result.getValue("alarm".getBytes(), "terminalId".getBytes()));

                    return new Tuple2<>(terminalId + "_" + alarmType, 1);
                }
            }).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer integer, Integer integer2) throws Exception {
                    return integer + integer2;
                }
            }).mapToPair(new PairFunction<Tuple2<String, Integer>, String, Double>() {
                @Override
                public Tuple2<String, Double> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                    float speed = Float.parseFloat(configlist.get(0));
                    BigDecimal myspeed = BigDecimal.valueOf(speed);
                    List<String> splitlist = Arrays.asList(stringIntegerTuple2._1.split("_"));
                    //碰撞类型
                    int alarmtype = Integer.parseInt(splitlist.get(1));
                    //终端ID
                    String terminalID = splitlist.get(0);
                    //车辆的速度
                    float currentspeed = Float.parseFloat(run.get(2));
                    BigDecimal current0fspeed = BigDecimal.valueOf(speed);
                    float allmark = 0;
                    if (alarmtype == ADASEnum.FCW.getVaule()) {
//                        if ()
                    }
                    return null;
                }
            });
            stringDoubleJavaPairRDD.foreach(new VoidFunction<Tuple2<String, Double>>() {
                @Override
                public void call(Tuple2<String, Double> stringIntegerTuple2) throws Exception {
                    // 车辆的vehicleid
                    String vehicleid = list.get(3);
                    // 车辆的车牌号
                    String number_plate = list.get(1);
                    //车辆的组
                    String vehicle_group = list.get(list.size() - 1);
                }
            });
//            long count = stringIntegerJavaPairRDD.count();
//            System.out.println("---------------------");
//            System.out.println(count);
        }

    }
}
