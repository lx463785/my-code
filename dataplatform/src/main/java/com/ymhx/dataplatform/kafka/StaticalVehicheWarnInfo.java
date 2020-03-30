package com.ymhx.dataplatform.kafka;

import com.ymhx.dataplatform.kafka.config.HbaseConfigMessage;
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
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.text.ParseException;
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
        //hbase配置

        //获取所有车辆的terminal_id
        List<Integer> list = new JdbcUtils().getterminalID();
        for (Integer terminalId : list) {
            //倒序
            String reverseId = StringUtils.reverse((""+terminalId));
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

            JavaPairRDD<ImmutableBytesWritable, Result> javaPairRDD = context.newAPIHadoopRDD(hconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

            JavaPairRDD<String, Integer> stringIntegerJavaPairRDD = javaPairRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, Integer>() {
                @Override
                public Tuple2<String, Integer> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                    Result result = immutableBytesWritableResultTuple2._2();

                    byte[] value = result.getValue("alarm".getBytes(), "alarmType".getBytes());
                    String s1 = Bytes.toString(value);
                    System.out.println(s1);
//                    Integer integer = Integer.valueOf(String.valueOf(result.getValue("alarm".getBytes(), "alarmType".getBytes())));
//                    System.out.println(integer);
                    System.out.println("---------------------");
                String s = new String(immutableBytesWritableResultTuple2._2().getValue("gps".getBytes(), "terminalId".getBytes()));
                System.out.println(s);
                    System.out.println("--------------------");
                    return null;
                }
            });
            long count = stringIntegerJavaPairRDD.count();
            System.out.println("---------------------");
            System.out.println(count);
        }

    }
}
