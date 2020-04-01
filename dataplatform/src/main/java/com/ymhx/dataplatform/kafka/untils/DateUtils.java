package com.ymhx.dataplatform.kafka.untils;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DateUtils implements Serializable {


    /**
     * 获取前一天的时间（0.00-24.00）
     */
    public static Map<String, Long> getBeforeOneDay() throws ParseException {

       DateFormat dateFmt = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        Date dNow = new Date(); //当前时间

        Date dBefore = new Date();

        Calendar calendar = Calendar.getInstance(); //得到日历

        calendar.setTime(dNow);//把当前时间赋给日历
        calendar.add(Calendar.MONTH, -5);
        calendar.add(Calendar.DAY_OF_MONTH, -24); //设置为前一天

        dBefore = calendar.getTime(); //得到前一天的时间

        String defaultStartDate = dateFmt.format(dBefore); //格式化前一天

        defaultStartDate = defaultStartDate.substring(0,10)+" 00:00:00";
        long starttime = dateFmt.parse(defaultStartDate).getTime();
        
        String defaultEndDate = defaultStartDate.substring(0,10)+" 23:59:59";
        long endtime = dateFmt.parse(defaultEndDate).getTime();
        Map<String,Long> map = new HashMap<>();
        map.put("startTime",starttime);
        map.put("endTime",endtime);
    return map;
    }

    /**
     * 获取当前时间
     */
    public static Map<String, String> getcurrentTime() throws ParseException {

        DateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date dNow = new Date(); //当前时间


        String defaultStartDate = dateFmt.format(dNow);

        defaultStartDate = defaultStartDate.substring(0,10)+" 00:00:00";

        String defaultEndDate = defaultStartDate.substring(0,10)+" 23:59:59";
        Map<String,String> map = new HashMap<>();
        map.put("startTime",defaultStartDate);
        map.put("endTime",defaultEndDate);
        return map;
    }
    /**
     *
     * @param args
     */
    public static void main(String[] args) {

        Integer a = 12;
        System.out.println(String.format("%-10s", a).replace(' ', '0'));
    }
}
