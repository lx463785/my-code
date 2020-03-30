package com.ymhx.dataplatform.kafka.untils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class BeanUtils {

        //通过反射获取类的属性
    public static   List<String>   getAttribute(Class<?> cls){
        Field[] declaredFields = cls.getDeclaredFields();
        List<String> list = new ArrayList<>();
        for (Field declaredField : declaredFields) {
            String name = declaredField.getName();
            list.add(name);
        }
        return list;
    }

}
