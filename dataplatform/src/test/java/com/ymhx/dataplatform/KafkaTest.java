package com.ymhx.dataplatform;


import com.ymhx.dataplatform.kafka.KafkaMesConsumer;
import com.ymhx.dataplatform.kafka.StaticalVehicheWarnInfo;

import com.ymhx.dataplatform.kafka.StatictalVehicheInformation;
import com.ymhx.dataplatform.kafka.test.KfkaProducer;

import com.ymhx.dataplatform.kafka.untils.JdbcUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.List;


@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaTest {

        @Autowired
        private KafkaMesConsumer kafkaConsumer;
        @Autowired
        private KfkaProducer kfkaProducer;
        @Autowired
        private StatictalVehicheInformation statictalVehicheInformation;
        @Autowired
        private StaticalVehicheWarnInfo staticalVehicheWarnInfo;

        @Test
        public  void  product(){

                kfkaProducer.send();
        }
        @Test
        public  void  alarm(){
                kafkaConsumer.getVehicleAlarmInformation();
        }
        @Test
        public  void  reissue(){
                kafkaConsumer.getVehicleLocationReissue();
        }
        @Test
        public  void  location(){
                kafkaConsumer.getVehicleRealTimeLocation();
        }
        @Test
        public  void  register(){
                kafkaConsumer.getVehicleRegister();
        }
        @Test
        public  void  adas(){
                kafkaConsumer.getVehicleAdasInfo();
        }
        @Test
        public  void  dms(){
                kafkaConsumer.getVehicleDms();
        }
        @Test
        public  void  dmscopy(){
                kafkaConsumer.getlogin();
        }
        @Test
        public  void  getcount(){
                statictalVehicheInformation.getCountAlarmInformation();
        }

        @Test
        public  void  getReportMes() throws IOException, SQLException {
                statictalVehicheInformation.getMes();
        }
        @Test
        public  void  getWarnCoefficient() throws IOException, SQLException, ParseException {

//                        staticalVehicheWarnInfo.getWarnCoefficient();


        }
        @Test
        public  void  gettpenum() throws IOException, SQLException, ParseException {
                SparkConf conf = new SparkConf().setAppName("warn")
                        .setMaster("local[2]")
                        //序列化
                        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                conf.set("spark.driver.allowMultipleContexts", "true");
                JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(30));
                JavaSparkContext context = jsc.sparkContext();
                for (int i = 5; i < 112; i++) {
                        int integer = i;
                        staticalVehicheWarnInfo.getWarnCoefficient(i,context);
                        staticalVehicheWarnInfo.getAlarmTypeNum(i,context);
                        staticalVehicheWarnInfo.getMileageCount(i,context);
                }




        }
        @Test
        public  void  getMileageCount() throws IOException, SQLException, ParseException {




        }

}


