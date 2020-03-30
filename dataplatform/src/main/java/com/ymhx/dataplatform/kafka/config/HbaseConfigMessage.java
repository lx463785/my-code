package com.ymhx.dataplatform.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
@ConfigurationProperties(prefix = "hbase")
@PropertySource("classpath:customParameter.properties")
@Configuration
public class HbaseConfigMessage implements Serializable {

    private  String servers;
    private  String clientPort;

    public HbaseConfigMessage() {
    }

    public String getServers() {
        return servers;
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public String getClientPort() {
        return clientPort;
    }

    public void setClientPort(String clientPort) {
        this.clientPort = clientPort;
    }
}
