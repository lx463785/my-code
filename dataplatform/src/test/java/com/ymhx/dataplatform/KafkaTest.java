package com.ymhx.dataplatform;


import com.ymhx.dataplatform.kafka.KafkaMesConsumer;
import com.ymhx.dataplatform.kafka.StaticalVehicheWarnInfo;

import com.ymhx.dataplatform.kafka.StatictalVehicheInformation;
import com.ymhx.dataplatform.kafka.test.KfkaProducer;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;


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
                staticalVehicheWarnInfo.getWarnCoefficient();
        }
        @Test
        public  void  getMileageCount() throws IOException, SQLException, ParseException {
                staticalVehicheWarnInfo.getMileageCount();
        }

}


