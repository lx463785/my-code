package com.ymhx.dataplatform.kafka.pojo;


import java.io.Serializable;

public class Login   implements Serializable {

    /***
     * 用户ID
     */
    private long userId;

    /**
     * 登录密码
     */
    private String  password;

    /***
     * 从链IP
     */
    private String  clientIP;

    /***
     * 从链端口
     */
    private int   clientPort;

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getClientIP() {
        return clientIP;
    }

    public void setClientIP(String clientIP) {
        this.clientIP = clientIP;
    }

    public int getClientPort() {
        return clientPort;
    }

    public void setClientPort(int clientPort) {
        this.clientPort = clientPort;
    }
}
