package com.ymhx.dataplatform.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component()
@ConfigurationProperties(prefix = "bigdata.config")
public class BigDataConfig {
    private Map<String,String> hBaseConfigMap;
    public Map<String, String> gethBaseConfigMap() {
        return hBaseConfigMap;
    }
    public void sethBaseConfigMap(Map<String, String> hBaseConfigMap) {
        this.hBaseConfigMap = hBaseConfigMap;
    }
}
