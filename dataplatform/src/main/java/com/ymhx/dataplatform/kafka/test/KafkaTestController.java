package com.ymhx.dataplatform.kafka.test;


import com.ymhx.dataplatform.kafka.KafkaMesConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
@Controller
public class KafkaTestController {

    @Autowired
    private KafkaMesConsumer consumer;

    @RequestMapping("/hello")
    @ResponseBody

    public String testSendMsg() throws InterruptedException {
        consumer.getVehicleAlarmInformation();

        return "success";
    }
    @RequestMapping("/hello1")
    @ResponseBody

    public String testSendMsg1() throws InterruptedException {

        consumer.getVehicleLocationReissue();
        return "success";
    }
}
