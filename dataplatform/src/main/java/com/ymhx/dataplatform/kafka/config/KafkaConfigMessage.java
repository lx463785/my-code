package com.ymhx.dataplatform.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Component
@ConfigurationProperties(prefix = "kafka")
@PropertySource("classpath: blaze.properties")
@Configuration
public class KafkaConfigMessage implements Serializable {

    private  String servers;
    private  String topic;
    private  String groupid;

    public String getServers() {
        return servers;
    }

    public KafkaConfigMessage() {
    }

    public void setServers(String servers) {
        this.servers = servers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroupid() {
        return groupid;
    }

    public void setGroupid(String groupid) {
        this.groupid = groupid;
    }

    public KafkaConfigMessage(String servers, String topic, String groupid) {
        this.servers = servers;
        this.topic = topic;
        this.groupid = groupid;
    }
}
